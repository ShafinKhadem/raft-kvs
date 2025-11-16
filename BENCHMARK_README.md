# Raft KVS Performance Benchmark Tool

This tool is used to conduct comprehensive performance benchmarking of the Raft KVS system and generate professional visualization reports.

## Features

### Performance Metrics Collection

1. **Throughput Metrics**
   - Operations per second (ops/sec)
   - Put/Get operation throughput

2. **Latency Metrics**
   - Average latency
   - P50/P95/P99 latency percentiles

3. **Reliability Metrics**
   - Success rate (OK responses)
   - Retry rate (ErrMaybe)
   - Version conflict rate (ErrVersion)

4. **Scalability Tests**
   - Performance with different client counts
   - Performance with different server counts
   - Network reliability tests

## Usage

### 1. Run Benchmark Tests

First ensure your project is properly built, then run the benchmark script:

```bash
# Add execute permission
chmod +x benchmark.sh

# Run benchmark
./benchmark.sh
```

The benchmark will automatically execute the following tests:
- Basic functionality tests
- Concurrent tests with different client counts (1, 2, 4, 8, 16, 32 clients)
- Scalability tests with different server counts (4, 8, 16, 32 servers)
- Fixed operation count tests (500, 1000, 2000, 5000 operations)
- Network unreliability tests

Test results will be saved in the `benchmark_results/YYYYMMDD_HHMMSS/` directory.

### 2. Generate Visualization Charts

After benchmarking completes, run the plotting script to generate performance charts:

```bash
# Install dependencies (if not already installed)
pip3 install matplotlib numpy

# Generate charts
python3 plot_results.py benchmark_results/YYYYMMDD_HHMMSS/
```

### 3. View Results

Generated charts include:

1. **throughput_vs_clients.png** - Throughput vs Client Count
2. **latency_vs_clients.png** - Latency vs Client Count
3. **throughput_vs_servers.png** - Throughput vs Server Count
4. **success_rate.png** - Operation Success Rate Bar Chart
5. **operation_breakdown.png** - Operation Type Distribution
6. **latency_cdf.png** - Latency Cumulative Distribution Function
7. **scalability_heatmap.png** - Scalability Heatmap
8. **performance_summary.txt** - Performance Summary in Text Format

All charts are saved in the `benchmark_results/YYYYMMDD_HHMMSS/plots/` directory.

## Custom Tests

### Modify Test Parameters

Edit the `benchmark.sh` file to customize test parameters:

```bash
# Modify client count range
for clients in 1 5 10 15 20 30; do
    run_throughput_benchmark ${clients} 5 0 10
done

# Modify server count range
for servers in 3 5 7 9; do
    run_throughput_benchmark 5 ${servers} 0 10
done

# Modify test duration (seconds)
run_throughput_benchmark 5 5 0 30  # Run for 30 seconds
```

### Add Custom Tests

You can add your own test scenarios in `benchmark.sh`:

```bash
# Example: High load stress test
log ">>> High Load Stress Test"
run_throughput_benchmark 50 5 0 60  # 50 clients, 60 seconds
```

## Output File Structure

```
benchmark_results/
└── YYYYMMDD_HHMMSS/
    ├── benchmark.log                          # Test log
    ├── summary.json                           # Test summary
    ├── throughput_c*_s*_ops*.json            # Performance data in JSON format
    ├── TestBasic4B_*.txt                     # Raw test output
    └── plots/                                 # Generated charts
        ├── throughput_vs_clients.png
        ├── latency_vs_clients.png
        ├── throughput_vs_servers.png
        ├── success_rate.png
        ├── operation_breakdown.png
        ├── latency_cdf.png
        ├── scalability_heatmap.png
        └── performance_summary.txt
```

## Performance Data Format

Each benchmark test's JSON output contains the following fields:

```json
{
    "num_clients": 5,
    "num_servers": 5,
    "num_operations": 10234,
    "duration_sec": 10.5,
    "throughput_ops_per_sec": 974.67,
    "avg_latency_ms": 12.34,
    "p50_latency_ms": 10.21,
    "p95_latency_ms": 25.67,
    "p99_latency_ms": 45.89,
    "success_rate": 98.5,
    "ok_count": 10080,
    "maybe_count": 154,
    "version_err_count": 0,
    "timestamp": "2025-11-16T10:30:45Z"
}
```

## Optimization Recommendations

Based on benchmark results, you can:

1. **Identify Performance Bottlenecks**
   - Review throughput vs client count chart to find optimal client count
   - Observe latency curves to identify system saturation point

2. **Optimize Configuration**
   - Choose optimal client/server ratio based on heatmap
   - Analyze success rate and operation distribution to adjust retry strategy

3. **Code Optimization Direction**
   - High P99 latency → Optimize slow paths
   - Low success rate → Improve conflict handling
   - Throughput doesn't increase with clients → Check for lock contention

## Troubleshooting

### Test Failures

If tests fail, check the `benchmark.log` file for detailed error information.

### Missing Data Points

If some charts are missing data, ensure:
- Tests completed successfully
- JSON files are properly formatted
- There are enough test data points with different configurations

### Chart Generation Errors

Ensure required Python packages are installed:
```bash
pip3 install matplotlib numpy
```

## Integration with Other Tools

### Export Data to CSV

```bash
# 将 JSON 数据转换为 CSV
python3 -c "
import json, csv, glob
files = glob.glob('benchmark_results/*/throughput_*.json')
with open('results.csv', 'w') as f:
    writer = csv.DictWriter(f, fieldnames=['num_clients', 'num_servers', 'throughput_ops_per_sec', 'avg_latency_ms'])
    writer.writeheader()
    for file in files:
        with open(file) as jf:
            writer.writerow(json.load(jf))
"
```


## License

This tool follows the main project license.
