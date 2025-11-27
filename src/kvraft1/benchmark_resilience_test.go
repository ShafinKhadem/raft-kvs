package kvraft

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"raft/rpc"
)

type ResilienceResult struct {
	NumClients        int     `json:"num_clients"`
	NumServers        int     `json:"num_servers"`
	FailureType       string  `json:"failure_type"`
	FailureRate       int     `json:"failure_rate_per_min"`
	DurationSec       float64 `json:"duration_sec"`
	NumOperations     int64   `json:"num_operations"`
	NumFailures       int     `json:"num_failures_injected"`
	ThroughputOps     float64 `json:"throughput_ops_per_sec"`
	AvgLatencyMs      float64 `json:"avg_latency_ms"`
	P50LatencyMs      float64 `json:"p50_latency_ms"`
	P95LatencyMs      float64 `json:"p95_latency_ms"`
	P99LatencyMs      float64 `json:"p99_latency_ms"`
	SuccessRate       float64 `json:"success_rate"`
	OkCount           int64   `json:"ok_count"`
	MaybeCount        int64   `json:"maybe_count"`
	VersionErrCount   int64   `json:"version_err_count"`
	FailureRecoveryMs float64 `json:"avg_failure_recovery_ms"`
	Timestamp         string  `json:"timestamp"`
}

func TestResilienceBenchmark(t *testing.T) {
	numClients := RESILIENCE_NUM_CLIENTS
	numServers := RESILIENCE_NUM_SERVERS
	durationSec := RESILIENCE_DURATION_SEC
	failureType := "crash"
	failureRate := RESILIENCE_FAILURE_RATE

	ts := MakeTest(t, "Resilience Benchmark", numClients, numServers, true, false, false, -1, false)
	defer ts.Cleanup()

	// Wait for system to be ready
	ck := ts.MakeClerk()
	ck.Get("warmup")

	var opsCompleted int64
	var okCount, maybeCount, versionErrCount int64
	var totalLatency int64
	var latencies []int64
	var latencyMu sync.Mutex
	var failureCount int32
	var totalRecoveryTime int64

	startTime := time.Now()
	deadline := startTime.Add(time.Duration(durationSec) * time.Second)

	var wg sync.WaitGroup
	var stopFailures int32

	// Failure injection goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()

		if failureRate <= 0 {
			return
		}

		failureInterval := time.Duration(60.0 / float64(failureRate) * float64(time.Second))
		ticker := time.NewTicker(failureInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if atomic.LoadInt32(&stopFailures) == 1 {
					return
				}

				failStart := time.Now()

				switch failureType {
				case "crash":
					// Randomly crash and restart a server
					serverIdx := rand.Intn(numServers)
					ts.Group(Gid).ShutdownServer(serverIdx)
					time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
					ts.Group(Gid).StartServer(serverIdx)
					atomic.AddInt32(&failureCount, 1)

				case "partition":
					// Create network partition
					partition1 := make([]int, numServers/2)
					partition2 := make([]int, numServers-numServers/2)
					for i := 0; i < numServers/2; i++ {
						partition1[i] = i
					}
					for i := numServers / 2; i < numServers; i++ {
						partition2[i-numServers/2] = i
					}
					ts.Group(Gid).Partition(partition1, partition2)
					time.Sleep(time.Duration(500+rand.Intn(1500)) * time.Millisecond)
					ts.Group(Gid).ConnectAll()
					atomic.AddInt32(&failureCount, 1)

				case "mixed":
					// Randomly choose between crash and partition
					if rand.Intn(2) == 0 {
						// Crash
						serverIdx := rand.Intn(numServers)
						ts.Group(Gid).ShutdownServer(serverIdx)
						time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
						ts.Group(Gid).StartServer(serverIdx)
					} else {
						// Partition
						partition1 := make([]int, numServers/2)
						partition2 := make([]int, numServers-numServers/2)
						for i := 0; i < numServers/2; i++ {
							partition1[i] = i
						}
						for i := numServers / 2; i < numServers; i++ {
							partition2[i-numServers/2] = i
						}
						ts.Group(Gid).Partition(partition1, partition2)
						time.Sleep(time.Duration(500+rand.Intn(1500)) * time.Millisecond)
						ts.Group(Gid).ConnectAll()
					}
					atomic.AddInt32(&failureCount, 1)
				}

				recoveryTime := time.Since(failStart).Milliseconds()
				atomic.AddInt64(&totalRecoveryTime, recoveryTime)
			}
		}
	}()

	// Client workload goroutines
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			ck := ts.MakeClerk()
			version := rpc.Tversion(0)
			opCount := 0

			for time.Now().Before(deadline) {
				key := fmt.Sprintf("key_%d", clientID)
				value := fmt.Sprintf("value_%d_%d", clientID, opCount)

				opStart := time.Now()
				err := ck.Put(key, value, version)
				latency := time.Since(opStart).Microseconds()

				atomic.AddInt64(&totalLatency, latency)
				latencyMu.Lock()
				latencies = append(latencies, latency)
				latencyMu.Unlock()

				if err == rpc.OK {
					atomic.AddInt64(&okCount, 1)
					version++
				} else if err == rpc.ErrMaybe {
					atomic.AddInt64(&maybeCount, 1)
					version++
				} else if err == rpc.ErrVersion {
					atomic.AddInt64(&versionErrCount, 1)
					if _, ver, gerr := ck.Get(key); gerr == rpc.OK {
						version = ver
					}
				}

				atomic.AddInt64(&opsCompleted, 1)
				opCount++
			}
		}(i)
	}

	// Wait for test duration
	time.Sleep(time.Duration(durationSec) * time.Second)
	atomic.StoreInt32(&stopFailures, 1)

	wg.Wait()
	duration := time.Since(startTime).Seconds()

	// Calculate statistics
	ops := atomic.LoadInt64(&opsCompleted)
	throughput := float64(ops) / duration
	avgLatency := float64(atomic.LoadInt64(&totalLatency)) / float64(ops) / 1000.0

	latencyMu.Lock()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50Latency := percentile(latencies, 50) / 1000.0
	p95Latency := percentile(latencies, 95) / 1000.0
	p99Latency := percentile(latencies, 99) / 1000.0
	latencyMu.Unlock()

	ok := atomic.LoadInt64(&okCount)
	maybe := atomic.LoadInt64(&maybeCount)
	verErr := atomic.LoadInt64(&versionErrCount)
	successRate := float64(ok) / float64(ops) * 100.0

	failures := int(atomic.LoadInt32(&failureCount))
	avgRecovery := 0.0
	if failures > 0 {
		avgRecovery = float64(atomic.LoadInt64(&totalRecoveryTime)) / float64(failures)
	}

	result := ResilienceResult{
		NumClients:        numClients,
		NumServers:        numServers,
		FailureType:       failureType,
		FailureRate:       failureRate,
		DurationSec:       duration,
		NumOperations:     ops,
		NumFailures:       failures,
		ThroughputOps:     throughput,
		AvgLatencyMs:      avgLatency,
		P50LatencyMs:      p50Latency,
		P95LatencyMs:      p95Latency,
		P99LatencyMs:      p99Latency,
		SuccessRate:       successRate,
		OkCount:           ok,
		MaybeCount:        maybe,
		VersionErrCount:   verErr,
		FailureRecoveryMs: avgRecovery,
		Timestamp:         time.Now().Format(time.RFC3339),
	}

	// Output JSON result
	jsonData, _ := json.MarshalIndent(result, "", "  ")
	outputFile := os.Getenv("RESILIENCE_OUTPUT_FILE")
	if outputFile != "" {
		os.WriteFile(outputFile, jsonData, 0644)
	}

	fmt.Printf("\n=== Resilience Benchmark Results ===\n")
	fmt.Printf("Failure Type: %s, Rate: %d/min\n", failureType, failureRate)
	fmt.Printf("Failures Injected: %d\n", failures)
	fmt.Printf("Throughput: %.2f ops/sec\n", throughput)
	fmt.Printf("Avg Latency: %.2f ms (P50: %.2f, P95: %.2f, P99: %.2f)\n",
		avgLatency, p50Latency, p95Latency, p99Latency)
	fmt.Printf("Success Rate: %.2f%%\n", successRate)
	fmt.Printf("Avg Failure Recovery Time: %.2f ms\n", avgRecovery)
	fmt.Printf("Total Ops: %d (OK: %d, Maybe: %d, VersionErr: %d)\n", ops, ok, maybe, verErr)
}

// These constants will be replaced
const (
	RESILIENCE_NUM_CLIENTS  = 5
	RESILIENCE_NUM_SERVERS  = 9
	RESILIENCE_DURATION_SEC = 10
	RESILIENCE_FAILURE_RATE = 2
)
