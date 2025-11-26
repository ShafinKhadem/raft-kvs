package kvraft

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"raft/rpc"
)

type BenchmarkResult struct {
	NumClients      int     `json:"num_clients"`
	NumServers      int     `json:"num_servers"`
	NumOperations   int64   `json:"num_operations"`
	DurationSec     float64 `json:"duration_sec"`
	ThroughputOps   float64 `json:"throughput_ops_per_sec"`
	AvgLatencyMs    float64 `json:"avg_latency_ms"`
	P50LatencyMs    float64 `json:"p50_latency_ms"`
	P95LatencyMs    float64 `json:"p95_latency_ms"`
	P99LatencyMs    float64 `json:"p99_latency_ms"`
	SuccessRate     float64 `json:"success_rate"`
	OkCount         int64   `json:"ok_count"`
	MaybeCount      int64   `json:"maybe_count"`
	VersionErrCount int64   `json:"version_err_count"`
	Timestamp       string  `json:"timestamp"`
}

func TestThroughputBenchmark(t *testing.T) {
	numClients := BENCHMARK_NUM_CLIENTS
	numServers := BENCHMARK_NUM_SERVERS
	numOps := BENCHMARK_NUM_OPS
	durationSec := BENCHMARK_DURATION_SEC

	ts := MakeTest(t, "Throughput Benchmark", numClients, numServers, true, false, false, -1, false)
	defer ts.Cleanup()

	// Wait for system to be ready
	ck := ts.MakeClerk()
	ck.Get("warmup")

	var opsCompleted int64
	var okCount, maybeCount, versionErrCount int64
	var totalLatency int64 // microseconds
	var latencies []int64
	var latencyMu sync.Mutex

	startTime := time.Now()
	deadline := startTime.Add(time.Duration(durationSec) * time.Second)

	var wg sync.WaitGroup

	// Start multiple clients
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			ck := ts.MakeClerk()
			version := rpc.Tversion(0)
			opCount := 0

			// Continue until either time deadline is reached (if durationSec > 0) OR operation count is reached (if numOps > 0)
			for (durationSec > 0 && time.Now().Before(deadline)) || (numOps > 0 && atomic.LoadInt64(&opsCompleted) < int64(numOps)) {
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
					// Re-fetch version
					if _, ver, gerr := ck.Get(key); gerr == rpc.OK {
						version = ver
					}
				}

				atomic.AddInt64(&opsCompleted, 1)
				opCount++
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime).Seconds()

	// Calculate statistics
	ops := atomic.LoadInt64(&opsCompleted)
	throughput := float64(ops) / duration
	avgLatency := float64(atomic.LoadInt64(&totalLatency)) / float64(ops) / 1000.0 // convert to milliseconds

	// Calculate percentile latency
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

	result := BenchmarkResult{
		NumClients:      numClients,
		NumServers:      numServers,
		NumOperations:   ops,
		DurationSec:     duration,
		ThroughputOps:   throughput,
		AvgLatencyMs:    avgLatency,
		P50LatencyMs:    p50Latency,
		P95LatencyMs:    p95Latency,
		P99LatencyMs:    p99Latency,
		SuccessRate:     successRate,
		OkCount:         ok,
		MaybeCount:      maybe,
		VersionErrCount: verErr,
		Timestamp:       time.Now().Format(time.RFC3339),
	}

	// Output JSON result
	jsonData, _ := json.MarshalIndent(result, "", "  ")
	outputFile := os.Getenv("BENCHMARK_OUTPUT_FILE")
	if outputFile != "" {
		os.WriteFile(outputFile, jsonData, 0644)
	}

	fmt.Printf("\n=== Benchmark Results ===\n")
	fmt.Printf("Throughput: %.2f ops/sec\n", throughput)
	fmt.Printf("Avg Latency: %.2f ms\n", avgLatency)
	fmt.Printf("P50 Latency: %.2f ms\n", p50Latency)
	fmt.Printf("P95 Latency: %.2f ms\n", p95Latency)
	fmt.Printf("P99 Latency: %.2f ms\n", p99Latency)
	fmt.Printf("Success Rate: %.2f%%\n", successRate)
	fmt.Printf("Total Ops: %d (OK: %d, Maybe: %d, VersionErr: %d)\n", ops, ok, maybe, verErr)
}

// These constants will be replaced
const (
	BENCHMARK_NUM_CLIENTS  = 5
	BENCHMARK_NUM_SERVERS  = 5
	BENCHMARK_NUM_OPS      = 5000
	BENCHMARK_DURATION_SEC = 0
)
