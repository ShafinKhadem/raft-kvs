#!/bin/bash

# Raft KVS Performance Benchmark Script
# Used to collect performance data under various configurations

set -e

# Configuration parameters
RESULTS_DIR="benchmark_results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Get absolute path of the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}/${RESULTS_DIR}/${TIMESTAMP}"

# Create results directory
mkdir -p "${OUTPUT_DIR}"

# Log file
LOG_FILE="${OUTPUT_DIR}/benchmark.log"

# Logging function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"
}

# Function to parse test output
parse_test_output() {
    local test_name=$1
    local output_file=$2
    local result_file=$3
    
    # Extract key metrics
    local pass_fail=$(grep -o "PASS\|FAIL" "${output_file}" | head -1 || echo "UNKNOWN")
    local duration=$(grep -oE "ok.*[0-9]+\.[0-9]+s" "${output_file}" | grep -oE "[0-9]+\.[0-9]+s" || echo "0s")
    
    # Record results
    echo "${test_name},${pass_fail},${duration}" >> "${result_file}"
}

# Run a single benchmark test
run_benchmark() {
    local test_name=$1
    local num_clients=$2
    local num_servers=$3
    local reliable=$4
    local description=$5
    
    log "Running test: ${description}"
    log "  Test name: ${test_name}"
    log "  Clients: ${num_clients}, Servers: ${num_servers}, Reliable network: ${reliable}"
    
    local output_file="${OUTPUT_DIR}/${test_name}_c${num_clients}_s${num_servers}_r${reliable}.txt"
    local timing_file="${OUTPUT_DIR}/${test_name}_c${num_clients}_s${num_servers}_r${reliable}_timing.json"
    
    # Clean up any existing custom test files
    rm -f "${SCRIPT_DIR}/src/kvraft1/benchmark_custom_test.go" "${SCRIPT_DIR}/src/kvraft1/benchmark_custom_test.go.bak"
    
    # Run test and record time
    cd "${SCRIPT_DIR}/src/kvraft1"
    
    local start_time=$(date +%s.%N)
    
    # Run Go test
    if go test -run "${test_name}" -v -timeout 10m > "${output_file}" 2>&1; then
        log "  ✓ Test passed"
    else
        log "  ✗ Test failed"
    fi
    
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)
    
    # Create JSON format time record
    cat > "${timing_file}" <<EOF
{
    "test_name": "${test_name}",
    "description": "${description}",
    "num_clients": ${num_clients},
    "num_servers": ${num_servers},
    "reliable": ${reliable},
    "duration_seconds": ${duration},
    "timestamp": "$(date -Iseconds)"
}
EOF
    
    cd "${SCRIPT_DIR}"
    
    log "  Completion time: ${duration}s"
    echo ""
}

# Custom benchmark test - measure throughput and latency
run_throughput_benchmark() {
    local num_clients=$1
    local num_servers=$2
    local num_operations=$3
    local duration_sec=$4
    
    log "Running throughput benchmark"
    log "  Clients: ${num_clients}, Servers: ${num_servers}"
    log "  Operations: ${num_operations}, Duration: ${duration_sec}s"
    
    local output_file="${OUTPUT_DIR}/throughput_c${num_clients}_s${num_servers}_ops${num_operations}.json"
    
    # Create temporary Go test file
    cat > "${SCRIPT_DIR}/src/kvraft1/benchmark_custom_test.go" <<'EOF'
package kvraft

import (
    "encoding/json"
    "fmt"
    "os"
    "sync"
    "sync/atomic"
    "testing"
    "time"
    
    "6.5840/kvsrv1/rpc"
)

type BenchmarkResult struct {
    NumClients      int       `json:"num_clients"`
    NumServers      int       `json:"num_servers"`
    NumOperations   int64     `json:"num_operations"`
    DurationSec     float64   `json:"duration_sec"`
    ThroughputOps   float64   `json:"throughput_ops_per_sec"`
    AvgLatencyMs    float64   `json:"avg_latency_ms"`
    P50LatencyMs    float64   `json:"p50_latency_ms"`
    P95LatencyMs    float64   `json:"p95_latency_ms"`
    P99LatencyMs    float64   `json:"p99_latency_ms"`
    SuccessRate     float64   `json:"success_rate"`
    OkCount         int64     `json:"ok_count"`
    MaybeCount      int64     `json:"maybe_count"`
    VersionErrCount int64     `json:"version_err_count"`
    Timestamp       string    `json:"timestamp"`
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
    sortInt64Slice(latencies)
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

func sortInt64Slice(arr []int64) {
    // Simple bubble sort
    n := len(arr)
    for i := 0; i < n; i++ {
        for j := 0; j < n-i-1; j++ {
            if arr[j] > arr[j+1] {
                arr[j], arr[j+1] = arr[j+1], arr[j]
            }
        }
    }
}

func percentile(sorted []int64, p int) float64 {
    if len(sorted) == 0 {
        return 0
    }
    index := int(float64(len(sorted)) * float64(p) / 100.0)
    if index >= len(sorted) {
        index = len(sorted) - 1
    }
    return float64(sorted[index])
}

// These constants will be replaced
const (
    BENCHMARK_NUM_CLIENTS   = 5
    BENCHMARK_NUM_SERVERS   = 3
    BENCHMARK_NUM_OPS       = 1000
    BENCHMARK_DURATION_SEC  = 10
)
EOF

    # Replace constant values
    sed -i.bak "s/BENCHMARK_NUM_CLIENTS   = [0-9]*/BENCHMARK_NUM_CLIENTS   = ${num_clients}/" "${SCRIPT_DIR}/src/kvraft1/benchmark_custom_test.go"
    sed -i.bak "s/BENCHMARK_NUM_SERVERS   = [0-9]*/BENCHMARK_NUM_SERVERS   = ${num_servers}/" "${SCRIPT_DIR}/src/kvraft1/benchmark_custom_test.go"
    sed -i.bak "s/BENCHMARK_NUM_OPS       = [0-9]*/BENCHMARK_NUM_OPS       = ${num_operations}/" "${SCRIPT_DIR}/src/kvraft1/benchmark_custom_test.go"
    sed -i.bak "s/BENCHMARK_DURATION_SEC  = [0-9]*/BENCHMARK_DURATION_SEC  = ${duration_sec}/" "${SCRIPT_DIR}/src/kvraft1/benchmark_custom_test.go"
    
    # Run benchmark test
    cd "${SCRIPT_DIR}/src/kvraft1"
    BENCHMARK_OUTPUT_FILE="${output_file}" go test -run TestThroughputBenchmark -v -timeout 5m
    cd "${SCRIPT_DIR}"
    
    # Cleanup
    rm -f "${SCRIPT_DIR}/src/kvraft1/benchmark_custom_test.go" "${SCRIPT_DIR}/src/kvraft1/benchmark_custom_test.go.bak"
    
    log "  Results saved to: ${output_file}"
}

# ==================== Main Test Flow ====================

log "=========================================="
log "Starting Raft KVS Performance Benchmark"
log "Results directory: ${OUTPUT_DIR}"
log "=========================================="
echo ""

# Clean up any old temporary files
log "Cleaning up old temporary files..."
rm -f "${SCRIPT_DIR}/src/kvraft1/benchmark_custom_test.go" "${SCRIPT_DIR}/src/kvraft1/benchmark_custom_test.go.bak"

# 1. Basic functionality tests
log ">>> Phase 1: Basic Functionality Tests"
run_benchmark "TestBasic4B" 1 5 "true" "Basic functionality test (1 client, 5 servers)"
run_benchmark "TestSpeed4B" 1 3 "true" "Speed test (1 client, 3 servers)"

# 2. Concurrent performance tests - different client counts
log ">>> Phase 2: Concurrent Client Count Performance Tests"
for clients in 1 2 4 8 16 32; do
    run_throughput_benchmark ${clients} 4 0 10
done

# 3. Server scale tests - different server counts
log ">>> Phase 3: Server Scale Performance Tests"
for servers in 4 8 16 32; do
    run_throughput_benchmark 4 ${servers} 0 10
done

# 4. Fixed operation count tests
log ">>> Phase 4: Fixed Operation Count Performance Tests"
for ops in 500 1000 2000 5000; do
    run_throughput_benchmark 5 5 ${ops} 0
done

# 5. Reliability tests
log ">>> Phase 5: Network Reliability Tests"
run_benchmark "TestUnreliable4B" 5 5 "false" "Unreliable network test (5 clients, 5 servers)"

# Generate summary report
log "=========================================="
log "Generating Summary Report"
log "=========================================="

SUMMARY_FILE="${OUTPUT_DIR}/summary.json"

cat > "${SUMMARY_FILE}" <<EOF
{
    "benchmark_timestamp": "${TIMESTAMP}",
    "benchmark_date": "$(date -Iseconds)",
    "results_directory": "${OUTPUT_DIR}",
    "tests_completed": true,
    "configuration": {
        "test_types": ["basic", "concurrent_clients", "server_scale", "fixed_ops", "reliability"],
        "client_counts": [1, 2, 4, 8, 16, 32],
        "server_counts": [1, 2, 4, 8, 16, 32],
        "operation_counts": [500, 1000, 2000, 5000]
    }
}
EOF

log "Summary report generated: ${SUMMARY_FILE}"
log ""
log "=========================================="
log "Benchmark testing complete!"
log "Results saved in: ${OUTPUT_DIR}"
log "Log file: ${LOG_FILE}"
log "=========================================="

echo ""
echo "Next step: Run plot_results.py to generate performance charts"
echo "  python3 plot_results.py ${OUTPUT_DIR}"
