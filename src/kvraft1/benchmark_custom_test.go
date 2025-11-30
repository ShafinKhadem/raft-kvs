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

// BenchmarkConfig holds configuration for a benchmark run
type BenchmarkConfig struct {
	NumReplicas int    `json:"num_replicas"`
	WriteRatio  int    `json:"write_ratio"` // Percentage of operations that are writes
	DurationSec int    `json:"duration_sec"`
	OutputFile  string `json:"output_file"`
	NumClients  int    `json:"num_clients"` // Number of concurrent clients
}

// BenchmarkResult stores the results of a benchmark run
type BenchmarkResult struct {
	Version     string  `json:"version"`
	NumReplicas int     `json:"num_replicas"`
	WriteRatio  int     `json:"write_ratio"`
	NumClients  int     `json:"num_clients"`
	DurationSec float64 `json:"duration_sec"`

	// Throughput metrics
	TotalOps      int64   `json:"total_ops"`
	GetOps        int64   `json:"get_ops"`
	PutOps        int64   `json:"put_ops"`
	ThroughputOps float64 `json:"throughput_ops_per_sec"`
	GetThroughput float64 `json:"get_throughput_ops_per_sec"`
	PutThroughput float64 `json:"put_throughput_ops_per_sec"`

	// Latency metrics (milliseconds)
	AvgLatencyMs float64 `json:"avg_latency_ms"`
	P50LatencyMs float64 `json:"p50_latency_ms"`
	P95LatencyMs float64 `json:"p95_latency_ms"`
	P99LatencyMs float64 `json:"p99_latency_ms"`

	GetAvgLatencyMs float64 `json:"get_avg_latency_ms"`
	GetP50LatencyMs float64 `json:"get_p50_latency_ms"`
	GetP95LatencyMs float64 `json:"get_p95_latency_ms"`
	GetP99LatencyMs float64 `json:"get_p99_latency_ms"`

	PutAvgLatencyMs float64 `json:"put_avg_latency_ms"`
	PutP50LatencyMs float64 `json:"put_p50_latency_ms"`
	PutP95LatencyMs float64 `json:"put_p95_latency_ms"`
	PutP99LatencyMs float64 `json:"put_p99_latency_ms"`

	// Success metrics
	SuccessOps  int64   `json:"success_ops"`
	FailedOps   int64   `json:"failed_ops"`
	SuccessRate float64 `json:"success_rate"`

	Timestamp string `json:"timestamp"`
}

// TestRaftBenchmark runs a comprehensive benchmark test for Raft
func TestRaftBenchmark(t *testing.T) {
	// Load configuration from file or use defaults
	config := loadBenchmarkConfig()

	numReplicas := config.NumReplicas
	writeRatio := config.WriteRatio
	durationSec := config.DurationSec
	numClients := config.NumClients

	// Set default values if not specified
	if numClients == 0 {
		numClients = 5
	}

	t.Logf("Starting Raft Benchmark:")
	t.Logf("  Replicas: %d", numReplicas)
	t.Logf("  Write Ratio: %d%%", writeRatio)
	t.Logf("  Duration: %ds", durationSec)
	t.Logf("  Clients: %d", numClients)

	// Create test environment with the specified number of replicas
	// MakeTest(t, part, nclients, nservers, reliable, crash, partitions, maxraftstate, randomkeys)
	ts := MakeTest(t, "Benchmark", numClients, numReplicas, true, false, false, -1, false)
	defer ts.Cleanup()

	// Wait for system to be ready
	ck := ts.MakeClerk()
	ck.Get("warmup")
	time.Sleep(500 * time.Millisecond)

	// Metrics tracking
	var totalOps, getOps, putOps int64
	var successOps, failedOps int64
	var totalLatencyMicros, getLatencyMicros, putLatencyMicros int64

	var allLatencies, getLatencies, putLatencies []int64
	var latencyMu sync.Mutex

	startTime := time.Now()
	deadline := startTime.Add(time.Duration(durationSec) * time.Second)

	var wg sync.WaitGroup

	// Start multiple client goroutines
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			// Each client has its own clerk
			ck := ts.MakeClerk()
			version := rpc.Tversion(0)

			// Each client maintains its own state
			localRand := rand.New(rand.NewSource(time.Now().UnixNano() + int64(clientID)))
			keySpace := 1000 // Number of unique keys

			for time.Now().Before(deadline) {
				key := fmt.Sprintf("key_%d", localRand.Intn(keySpace))

				// Decide whether to do a Get or Put based on write ratio
				isWrite := localRand.Intn(100) < writeRatio

				opStart := time.Now()

				if isWrite {
					// Put operation
					value := fmt.Sprintf("value_%d_%d", clientID, time.Now().UnixNano())

					err := ck.Put(key, value, version)
					latency := time.Since(opStart).Microseconds()

					atomic.AddInt64(&putOps, 1)
					atomic.AddInt64(&putLatencyMicros, latency)

					latencyMu.Lock()
					putLatencies = append(putLatencies, latency)
					allLatencies = append(allLatencies, latency)
					latencyMu.Unlock()

					if err == rpc.OK {
						version++
						atomic.AddInt64(&successOps, 1)
					} else if err == rpc.ErrMaybe || err == rpc.ErrVersion {
						version++
						atomic.AddInt64(&successOps, 1)
					} else {
						atomic.AddInt64(&failedOps, 1)
					}

				} else {
					// Get operation - will use batched read optimization!
					_, _, err := ck.Get(key)
					latency := time.Since(opStart).Microseconds()

					atomic.AddInt64(&getOps, 1)
					atomic.AddInt64(&getLatencyMicros, latency)

					latencyMu.Lock()
					getLatencies = append(getLatencies, latency)
					allLatencies = append(allLatencies, latency)
					latencyMu.Unlock()

					if err == rpc.OK || err == rpc.ErrNoKey {
						atomic.AddInt64(&successOps, 1)
					} else {
						atomic.AddInt64(&failedOps, 1)
					}
				}

				atomic.AddInt64(&totalOps, 1)
				atomic.AddInt64(&totalLatencyMicros, time.Since(opStart).Microseconds())
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime).Seconds()

	// Calculate statistics
	ops := atomic.LoadInt64(&totalOps)
	gets := atomic.LoadInt64(&getOps)
	puts := atomic.LoadInt64(&putOps)
	success := atomic.LoadInt64(&successOps)
	failed := atomic.LoadInt64(&failedOps)

	throughput := float64(ops) / duration
	getThroughput := float64(gets) / duration
	putThroughput := float64(puts) / duration

	avgLatency := avgLatencyMs(atomic.LoadInt64(&totalLatencyMicros), ops)
	getAvgLatency := avgLatencyMs(atomic.LoadInt64(&getLatencyMicros), gets)
	putAvgLatency := avgLatencyMs(atomic.LoadInt64(&putLatencyMicros), puts)

	// Calculate percentile latencies
	latencyMu.Lock()
	sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })
	sort.Slice(getLatencies, func(i, j int) bool { return getLatencies[i] < getLatencies[j] })
	sort.Slice(putLatencies, func(i, j int) bool { return putLatencies[i] < putLatencies[j] })

	p50 := percentile(allLatencies, 50) / 1000.0
	p95 := percentile(allLatencies, 95) / 1000.0
	p99 := percentile(allLatencies, 99) / 1000.0

	getP50 := percentile(getLatencies, 50) / 1000.0
	getP95 := percentile(getLatencies, 95) / 1000.0
	getP99 := percentile(getLatencies, 99) / 1000.0

	putP50 := percentile(putLatencies, 50) / 1000.0
	putP95 := percentile(putLatencies, 95) / 1000.0
	putP99 := percentile(putLatencies, 99) / 1000.0
	latencyMu.Unlock()

	successRate := float64(success) / float64(ops) * 100.0

	// Create result structure
	result := BenchmarkResult{
		Version:         os.Getenv("RAFT_VERSION"),
		NumReplicas:     numReplicas,
		WriteRatio:      writeRatio,
		NumClients:      numClients,
		DurationSec:     duration,
		TotalOps:        ops,
		GetOps:          gets,
		PutOps:          puts,
		ThroughputOps:   throughput,
		GetThroughput:   getThroughput,
		PutThroughput:   putThroughput,
		AvgLatencyMs:    avgLatency,
		P50LatencyMs:    p50,
		P95LatencyMs:    p95,
		P99LatencyMs:    p99,
		GetAvgLatencyMs: getAvgLatency,
		GetP50LatencyMs: getP50,
		GetP95LatencyMs: getP95,
		GetP99LatencyMs: getP99,
		PutAvgLatencyMs: putAvgLatency,
		PutP50LatencyMs: putP50,
		PutP95LatencyMs: putP95,
		PutP99LatencyMs: putP99,
		SuccessOps:      success,
		FailedOps:       failed,
		SuccessRate:     successRate,
		Timestamp:       time.Now().Format(time.RFC3339),
	}

	// Save results to file
	if config.OutputFile != "" {
		saveResults(result, config.OutputFile)
	}

	// Print summary
	t.Logf("\n=== Benchmark Results ===")
	t.Logf("Total Throughput: %.2f ops/sec", throughput)
	t.Logf("  Get Throughput: %.2f ops/sec (%.1f%%)", getThroughput, float64(gets)/float64(ops)*100)
	t.Logf("  Put Throughput: %.2f ops/sec (%.1f%%)", putThroughput, float64(puts)/float64(ops)*100)
	t.Logf("\nLatency:")
	t.Logf("  Overall: Avg=%.2fms, P50=%.2fms, P95=%.2fms, P99=%.2fms", avgLatency, p50, p95, p99)
	t.Logf("  Get:     Avg=%.2fms, P50=%.2fms, P95=%.2fms, P99=%.2fms", getAvgLatency, getP50, getP95, getP99)
	t.Logf("  Put:     Avg=%.2fms, P50=%.2fms, P95=%.2fms, P99=%.2fms", putAvgLatency, putP50, putP95, putP99)
	t.Logf("\nSuccess Rate: %.2f%% (%d/%d)", successRate, success, ops)
}

func loadBenchmarkConfig() BenchmarkConfig {
	configFile := os.Getenv("BENCH_CONFIG_FILE")
	if configFile == "" {
		configFile = "/tmp/raft_bench_config.json"
	}

	data, err := os.ReadFile(configFile)
	if err != nil {
		// Return default config
		return BenchmarkConfig{
			NumReplicas: 5,
			WriteRatio:  10,
			DurationSec: 30,
			NumClients:  5,
		}
	}

	var config BenchmarkConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return BenchmarkConfig{
			NumReplicas: 5,
			WriteRatio:  10,
			DurationSec: 30,
			NumClients:  5,
		}
	}

	return config
}

func saveResults(result BenchmarkResult, filename string) {
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fmt.Printf("Error marshaling results: %v\n", err)
		return
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		fmt.Printf("Error writing results to %s: %v\n", filename, err)
		return
	}

	fmt.Printf("Results saved to: %s\n", filename)
}
