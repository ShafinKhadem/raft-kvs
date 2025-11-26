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
    rm -f "${SCRIPT_DIR}/src/kvraft1/benchmark_custom_test.go.bak"

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

    # Replace constant values
    sed -i.bak "s/BENCHMARK_NUM_CLIENTS  = [0-9]*/BENCHMARK_NUM_CLIENTS  = ${num_clients}/" "${SCRIPT_DIR}/src/kvraft1/benchmark_custom_test.go"
    sed -i.bak "s/BENCHMARK_NUM_SERVERS  = [0-9]*/BENCHMARK_NUM_SERVERS  = ${num_servers}/" "${SCRIPT_DIR}/src/kvraft1/benchmark_custom_test.go"
    sed -i.bak "s/BENCHMARK_NUM_OPS      = [0-9]*/BENCHMARK_NUM_OPS      = ${num_operations}/" "${SCRIPT_DIR}/src/kvraft1/benchmark_custom_test.go"
    sed -i.bak "s/BENCHMARK_DURATION_SEC = [0-9]*/BENCHMARK_DURATION_SEC = ${duration_sec}/" "${SCRIPT_DIR}/src/kvraft1/benchmark_custom_test.go"

    # Run benchmark test
    cd "${SCRIPT_DIR}/src/kvraft1"
    BENCHMARK_OUTPUT_FILE="${output_file}" go test -run TestThroughputBenchmark -v -timeout 5m
    cd "${SCRIPT_DIR}"

    # Cleanup
    rm -f "${SCRIPT_DIR}/src/kvraft1/benchmark_custom_test.go.bak"

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

    # Replace constant values
    sed -i.bak "s/RESILIENCE_NUM_CLIENTS  = [0-9]*/RESILIENCE_NUM_CLIENTS  = ${num_clients}/" "${SCRIPT_DIR}/src/kvraft1/benchmark_resilience_test.go"
    sed -i.bak "s/RESILIENCE_NUM_SERVERS  = [0-9]*/RESILIENCE_NUM_SERVERS  = ${num_servers}/" "${SCRIPT_DIR}/src/kvraft1/benchmark_resilience_test.go"
    sed -i.bak "s/RESILIENCE_DURATION_SEC = [0-9]*/RESILIENCE_DURATION_SEC = ${duration_sec}/" "${SCRIPT_DIR}/src/kvraft1/benchmark_resilience_test.go"
    sed -i.bak "s/RESILIENCE_FAILURE_RATE = [0-9]*/RESILIENCE_FAILURE_RATE = ${failure_rate}/" "${SCRIPT_DIR}/src/kvraft1/benchmark_resilience_test.go"
    sed -i.bak "s/\"RESILIENCE_FAILURE_TYPE\"/\"${failure_type}\"/" "${SCRIPT_DIR}/src/kvraft1/benchmark_resilience_test.go"

    # Run resilience benchmark test
    cd "${SCRIPT_DIR}/src/kvraft1"
    RESILIENCE_OUTPUT_FILE="${output_file}" go test -run TestResilienceBenchmark -v -timeout 10m
    cd "${SCRIPT_DIR}"

    # Cleanup
    rm -f "${SCRIPT_DIR}/src/kvraft1/benchmark_resilience_test.go.bak"

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
rm -f "${SCRIPT_DIR}/src/kvraft1/benchmark_custom_test.go.bak"
rm -f "${SCRIPT_DIR}/src/kvraft1/benchmark_resilience_test.go.bak"

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
