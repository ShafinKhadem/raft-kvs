#!/bin/bash

# Raft Performance Benchmark Script
# Compares performance across different Raft implementations
# Usage: ./raft_benchmark.sh <version_name> [output_dir]

set -e

VERSION_NAME=${1:-"base"}
RESULTS_DIR=${2:-"benchmark_results"}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Get absolute paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="${SCRIPT_DIR}/${RESULTS_DIR}/${VERSION_NAME}_${TIMESTAMP}"

# Create results directory
mkdir -p "${OUTPUT_DIR}"

# Log file
LOG_FILE="${OUTPUT_DIR}/benchmark.log"

# Logging function
log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "${LOG_FILE}"
}

log "=========================================="
log "Starting Raft Benchmark for: ${VERSION_NAME}"
log "Results directory: ${OUTPUT_DIR}"
log "=========================================="

# Configuration parameters for benchmarks
WRITE_RATIOS=(2 5 10 15 20 50 100)
REPLICA_COUNTS=(3 5 7)
DURATION_SEC=30

# Function to run a single benchmark test
run_benchmark_test() {
    local num_replicas=$1
    local write_ratio=$2
    local duration=$3
    local num_clients=${4:-5}
    local test_name="replicas${num_replicas}_writes${write_ratio}_clients${num_clients}"

    log "Running benchmark: ${test_name}"
    log "  Replicas: ${num_replicas}, Write Ratio: ${write_ratio}%, Duration: ${duration}s"

    local output_file="${OUTPUT_DIR}/${test_name}.json"

    # Run the benchmark (adjust this command based on your actual test setup)
    # This assumes you have a benchmark test that can be parameterized
    cd "${SCRIPT_DIR}/src/kvraft1"

    # Create a temporary test file with the configuration
    cat > /tmp/raft_bench_config.json <<EOF
{
    "num_replicas": ${num_replicas},
    "write_ratio": ${write_ratio},
    "duration_sec": ${duration},
    "output_file": "${output_file}",
    "num_clients": ${num_clients}
}
EOF
    cat /tmp/raft_bench_config.json

    # Run the Go benchmark test
    # Pass configuration via environment variable
    if BENCH_CONFIG_FILE=/tmp/raft_bench_config.json RAFT_VERSION="${VERSION_NAME}" \
        go test -run "^TestRaftBenchmark$" -timeout $((duration + 60))s -v 2>&1 | tee -a "${LOG_FILE}"; then
        log "  ✓ Benchmark completed successfully"
    else
        log "  ✗ Benchmark failed"
    fi

    rm -f /tmp/raft_bench_config.json
}

# Run benchmarks for different write ratios (varying write workload)
log ">>> Phase 1: Write Ratio Performance Tests (5 replicas)"
CLIENT_COUNTS=(4000)
for num_clients in "${CLIENT_COUNTS[@]}"; do
    run_benchmark_test 5 0 ${DURATION_SEC} ${num_clients}
done

# # Run benchmarks for different replica counts (scalability)
# log ">>> Phase 2: Replica Scalability Tests (2% writes)"
# for replicas in "${REPLICA_COUNTS[@]}"; do
#     run_benchmark_test ${replicas} 2 ${DURATION_SEC}
# done

# log ">>> Phase 3: Replica Scalability Tests (10% writes)"
# for replicas in "${REPLICA_COUNTS[@]}"; do
#     run_benchmark_test ${replicas} 10 ${DURATION_SEC}
# done

# Generate summary JSON
SUMMARY_FILE="${OUTPUT_DIR}/summary.json"
cat > "${SUMMARY_FILE}" <<EOF
{
    "version": "${VERSION_NAME}",
    "timestamp": "${TIMESTAMP}",
    "benchmark_date": "$(date -Iseconds)",
    "results_directory": "${OUTPUT_DIR}",
    "configuration": {
        "write_ratios": [$(IFS=,; echo "${WRITE_RATIOS[*]}")],
        "replica_counts": [$(IFS=,; echo "${REPLICA_COUNTS[*]}")],
        "duration_sec": ${DURATION_SEC}
    }
}
EOF

log "=========================================="
log "Benchmark complete for version: ${VERSION_NAME}"
log "Results saved to: ${OUTPUT_DIR}"
log "=========================================="
echo ""
echo "Summary file: ${SUMMARY_FILE}"
echo ""
echo "To plot results, run:"
echo "  python3 plot_raft_results.py ${RESULTS_DIR}"
