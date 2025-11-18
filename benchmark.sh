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
    "sort"
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

# Resilience benchmark test - measure throughput with failure injection
run_resilience_benchmark() {
    local num_clients=$1
    local num_servers=$2
    local duration_sec=$3
    local failure_type=$4  # "crash", "partition", "mixed"
    local failure_rate=$5  # failures per minute
    
    log "Running resilience benchmark with failure injection"
    log "  Clients: ${num_clients}, Servers: ${num_servers}"
    log "  Duration: ${duration_sec}s, Failure Type: ${failure_type}, Failure Rate: ${failure_rate}/min"
    
    local output_file="${OUTPUT_DIR}/resilience_c${num_clients}_s${num_servers}_f${failure_type}_r${failure_rate}.json"
    
    # Create temporary Go test file for resilience testing
    cat > "${SCRIPT_DIR}/src/kvraft1/benchmark_resilience_test.go" <<'RESEOF'
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
    
    "6.5840/kvsrv1/rpc"
)

type ResilienceResult struct {
    NumClients        int       `json:"num_clients"`
    NumServers        int       `json:"num_servers"`
    FailureType       string    `json:"failure_type"`
    FailureRate       int       `json:"failure_rate_per_min"`
    DurationSec       float64   `json:"duration_sec"`
    NumOperations     int64     `json:"num_operations"`
    NumFailures       int       `json:"num_failures_injected"`
    ThroughputOps     float64   `json:"throughput_ops_per_sec"`
    AvgLatencyMs      float64   `json:"avg_latency_ms"`
    P50LatencyMs      float64   `json:"p50_latency_ms"`
    P95LatencyMs      float64   `json:"p95_latency_ms"`
    P99LatencyMs      float64   `json:"p99_latency_ms"`
    SuccessRate       float64   `json:"success_rate"`
    OkCount           int64     `json:"ok_count"`
    MaybeCount        int64     `json:"maybe_count"`
    VersionErrCount   int64     `json:"version_err_count"`
    FailureRecoveryMs float64   `json:"avg_failure_recovery_ms"`
    Timestamp         string    `json:"timestamp"`
}

func TestResilienceBenchmark(t *testing.T) {
    numClients := RESILIENCE_NUM_CLIENTS
    numServers := RESILIENCE_NUM_SERVERS
    durationSec := RESILIENCE_DURATION_SEC
    failureType := "RESILIENCE_FAILURE_TYPE"
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
                    for i := numServers/2; i < numServers; i++ {
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
                        for i := numServers/2; i < numServers; i++ {
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
    RESILIENCE_NUM_CLIENTS   = 5
    RESILIENCE_NUM_SERVERS   = 5
    RESILIENCE_DURATION_SEC  = 30
    RESILIENCE_FAILURE_RATE  = 2
)
RESEOF

    # Replace constant values
    sed -i.bak "s/RESILIENCE_NUM_CLIENTS   = [0-9]*/RESILIENCE_NUM_CLIENTS   = ${num_clients}/" "${SCRIPT_DIR}/src/kvraft1/benchmark_resilience_test.go"
    sed -i.bak "s/RESILIENCE_NUM_SERVERS   = [0-9]*/RESILIENCE_NUM_SERVERS   = ${num_servers}/" "${SCRIPT_DIR}/src/kvraft1/benchmark_resilience_test.go"
    sed -i.bak "s/RESILIENCE_DURATION_SEC  = [0-9]*/RESILIENCE_DURATION_SEC  = ${duration_sec}/" "${SCRIPT_DIR}/src/kvraft1/benchmark_resilience_test.go"
    sed -i.bak "s/RESILIENCE_FAILURE_RATE  = [0-9]*/RESILIENCE_FAILURE_RATE  = ${failure_rate}/" "${SCRIPT_DIR}/src/kvraft1/benchmark_resilience_test.go"
    sed -i.bak "s/\"RESILIENCE_FAILURE_TYPE\"/\"${failure_type}\"/" "${SCRIPT_DIR}/src/kvraft1/benchmark_resilience_test.go"
    
    # Run resilience benchmark test
    cd "${SCRIPT_DIR}/src/kvraft1"
    RESILIENCE_OUTPUT_FILE="${output_file}" go test -run TestResilienceBenchmark -v -timeout 10m
    cd "${SCRIPT_DIR}"
    
    # Cleanup
    rm -f "${SCRIPT_DIR}/src/kvraft1/benchmark_resilience_test.go" "${SCRIPT_DIR}/src/kvraft1/benchmark_resilience_test.go.bak"
    
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
rm -f "${SCRIPT_DIR}/src/kvraft1/benchmark_resilience_test.go" "${SCRIPT_DIR}/src/kvraft1/benchmark_resilience_test.go.bak"

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

# 6. Resilience tests with failure injection
log ">>> Phase 6: Resilience Tests with Failure Injection"

# 6a. Server crash resilience
log ">> Phase 6a: Server Crash Resilience Tests"
run_resilience_benchmark 5 5 10 "crash" 1  # 1 crash per minute
run_resilience_benchmark 5 5 10 "crash" 2  # 2 crashes per minute
run_resilience_benchmark 5 5 10 "crash" 4  # 4 crashes per minute

# 6b. Network partition resilience
log ">> Phase 6b: Network Partition Resilience Tests"
run_resilience_benchmark 5 5 10 "partition" 1  # 1 partition per minute
run_resilience_benchmark 5 5 10 "partition" 2  # 2 partitions per minute
run_resilience_benchmark 5 5 10 "partition" 4  # 4 partitions per minute

# 6c. Mixed failure resilience
log ">> Phase 6c: Mixed Failure Resilience Tests"
run_resilience_benchmark 5 5 10 "mixed" 2  # 2 mixed failures per minute
run_resilience_benchmark 5 5 10 "mixed" 4  # 4 mixed failures per minute
run_resilience_benchmark 5 5 10 "mixed" 6  # 6 mixed failures per minute

# 6d. Scale resilience tests - varying number of clients under failures
log ">> Phase 6d: Client Scale Resilience Tests"
for clients in 2 4 8 16; do
    run_resilience_benchmark ${clients} 5 10 "mixed" 2
done

# 6e. Server scale resilience tests - varying number of servers under failures
log ">> Phase 6e: Server Scale Resilience Tests"
for servers in 3 5 7 9; do
    run_resilience_benchmark 5 ${servers} 10 "crash" 2
done

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
        "test_types": ["basic", "concurrent_clients", "server_scale", "fixed_ops", "reliability", "resilience"],
        "client_counts": [1, 2, 4, 8, 16, 32],
        "server_counts": [1, 2, 4, 8, 16, 32],
        "operation_counts": [500, 1000, 2000, 5000],
        "resilience_tests": {
            "failure_types": ["crash", "partition", "mixed"],
            "failure_rates": [1, 2, 4, 6],
            "test_duration_sec": 30
        }
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
