#!/usr/bin/env python3
"""
Raft Benchmark Results Plotter

Generates performance comparison plots similar to:
- Figure 5: Throughput vs. Write Ratio
- Figure 7: Scalability with varying replicas

Usage:
    python3 plot_raft_results.py <benchmark_results_dir>
"""

import json
import os
import sys
import glob
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

# Matplotlib configuration for publication-quality plots
plt.rcParams['figure.figsize'] = (10, 6)
plt.rcParams['font.size'] = 12
plt.rcParams['axes.labelsize'] = 14
plt.rcParams['axes.titlesize'] = 16
plt.rcParams['xtick.labelsize'] = 12
plt.rcParams['ytick.labelsize'] = 12
plt.rcParams['legend.fontsize'] = 12
plt.rcParams['lines.linewidth'] = 2
plt.rcParams['lines.markersize'] = 8


def load_benchmark_results(results_dir):
    """
    Load all benchmark results from JSON files in the results directory.
    
    Returns a dictionary organized by version, then by test parameters.
    """
    results = defaultdict(list)
    
    # Find all version subdirectories
    version_dirs = glob.glob(os.path.join(results_dir, "*_*"))
    
    for version_dir in version_dirs:
        # Extract version name from directory name (before timestamp)
        version_name = os.path.basename(version_dir).rsplit('_', 2)[0]
        
        # Load all JSON result files in this directory
        json_files = glob.glob(os.path.join(version_dir, "*.json"))
        
        for json_file in json_files:
            # Skip summary files
            if "summary" in json_file:
                continue
                
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    data['version'] = version_name  # Ensure version is set
                    results[version_name].append(data)
            except Exception as e:
                print(f"Warning: Failed to load {json_file}: {e}")
    
    return dict(results)


def plot_throughput_vs_write_ratio(results, output_dir):
    """
    Plot Figure 5 style: Throughput vs. Write Ratio
    Shows how throughput changes with different write percentages
    """
    plt.figure(figsize=(10, 6))
    
    # Extract data for each version (filter for 5 replicas)
    for version_name, version_data in sorted(results.items()):
        # Filter for results with 5 replicas
        filtered = [d for d in version_data if d.get('num_replicas') == 5]
        
        if not filtered:
            continue
        
        # Sort by write ratio
        filtered.sort(key=lambda x: x.get('write_ratio', 0))
        
        # Skip if no valid data
        if not filtered:
            continue
        
        write_ratios = [d['write_ratio'] for d in filtered if 'write_ratio' in d and 'throughput_ops_per_sec' in d]
        throughputs = [d['throughput_ops_per_sec'] for d in filtered if 'write_ratio' in d and 'throughput_ops_per_sec' in d]
        
        if not write_ratios or not throughputs:
            continue
        
        # Plot with markers
        marker = 'o' if 'ALR' in version_name or 'alr' in version_name else 'x'
        plt.plot(write_ratios, throughputs, marker=marker, label=version_name, linewidth=2, markersize=10)
    
    plt.xlabel('% write ratio', fontsize=14)
    plt.ylabel('Requests / sec', fontsize=14)
    plt.title('Throughput vs. Write Ratio (5 replicas)', fontsize=16)
    plt.grid(True, alpha=0.3)
    plt.legend(loc='upper right', fontsize=12)
    
    # Use log scale if throughput varies significantly
    all_throughputs = [d['throughput_ops_per_sec'] for v in results.values() for d in v if 'throughput_ops_per_sec' in d]
    if all_throughputs:
        max_throughput = max(all_throughputs)
        min_throughput = min(all_throughputs)
        if max_throughput / min_throughput > 10:
            plt.yscale('log')
    
    plt.tight_layout()
    output_file = os.path.join(output_dir, 'figure5_throughput_vs_write_ratio.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Saved: {output_file}")
    plt.close()


def plot_scalability_replicas(results, output_dir):
    """
    Plot Figure 7 style: Scalability with varying number of replicas
    Shows performance for both 2% and 10% write workloads
    Bars are grouped by version (baseline first), with colors representing different replica counts
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))
    
    write_ratios_to_plot = [2, 10]
    axes = [ax1, ax2]
    
    # Define version order: baseline first, then others
    version_order = ['base', 'ALR-leader-only', 'ALR']
    version_names = []
    for v in version_order:
        if v in results:
            version_names.append(v)
    # Add any other versions not in the predefined order
    for v in sorted(results.keys()):
        if v not in version_names:
            version_names.append(v)
    
    for write_ratio, ax in zip(write_ratios_to_plot, axes):
        # Get all replica counts
        replica_counts = sorted(set(d.get('num_replicas', 0) 
                                   for v in results.values() 
                                   for d in v))
        
        # Color scheme for different replica counts
        colors = {3: '#FF9D66', 5: '#6699CC', 7: '#66CC99'}
        
        # Bar width and spacing
        num_replicas = len(replica_counts)
        bar_width = 0.8 / num_replicas
        
        # Track which replica counts have been added to legend
        legend_added = set()
        
        for i, version_name in enumerate(version_names):
            version_data = results[version_name]
            
            # Filter for this write ratio
            filtered = [d for d in version_data if d.get('write_ratio') == write_ratio]
            
            # Organize by replica count
            throughput_by_replicas = {}
            for d in filtered:
                replicas = d.get('num_replicas')
                throughput = d.get('throughput_ops_per_sec', 0)
                throughput_by_replicas[replicas] = throughput
            
            # Plot bars for each replica count
            for j, replicas in enumerate(replica_counts):
                if replicas in throughput_by_replicas:
                    x_pos = i + j * bar_width
                    height = throughput_by_replicas[replicas]
                    color = colors.get(replicas, '#CCCCCC')
                    
                    # Only add label for legend once per replica count
                    label = f'{replicas} replicas' if replicas not in legend_added else None
                    if label:
                        legend_added.add(replicas)
                    
                    ax.bar(x_pos, height, bar_width, 
                          label=label,
                          color=color,
                          alpha=0.8)
        
        # Configure axis
        ax.set_xlabel('Version', fontsize=14)
        ax.set_ylabel('Requests / sec', fontsize=14)
        ax.set_title(f'{write_ratio}% writes', fontsize=16)
        ax.set_xticks([i + bar_width * (num_replicas - 1) / 2 
                       for i in range(len(version_names))])
        ax.set_xticklabels(version_names)
        ax.grid(True, alpha=0.3, axis='y')
        ax.legend(loc='upper left', fontsize=10)
    
    plt.tight_layout()
    output_file = os.path.join(output_dir, 'figure7_scalability_replicas.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Saved: {output_file}")
    plt.close()


def plot_latency_comparison(results, output_dir):
    """
    Additional plot: Latency comparison across versions
    """
    plt.figure(figsize=(12, 6))
    
    # Define version order: baseline first, then others
    version_order = ['base', 'ALR-leader-only', 'ALR']
    version_names = []
    for v in version_order:
        if v in results:
            version_names.append(v)
    # Add any other versions not in the predefined order
    for v in sorted(results.keys()):
        if v not in version_names:
            version_names.append(v)
    
    # Focus on 5 replicas, 10% writes for fair comparison
    for i, version_name in enumerate(version_names):
        version_data = results[version_name]
        filtered = [d for d in version_data 
                   if d.get('num_replicas') == 5 and d.get('write_ratio') == 10]
        
        if not filtered:
            continue
        
        # Get latency percentiles
        data = filtered[0]
        percentiles = ['P50', 'P95', 'P99']
        latencies = [
            data.get('p50_latency_ms', 0),
            data.get('p95_latency_ms', 0),
            data.get('p99_latency_ms', 0)
        ]
        
        x_pos = np.arange(len(percentiles))
        plt.bar(x_pos + 0.2 * i, 
               latencies, 
               width=0.2,
               label=version_name,
               alpha=0.8)
    
    plt.xlabel('Latency Percentile', fontsize=14)
    plt.ylabel('Latency (ms)', fontsize=14)
    plt.title('Latency Comparison (5 replicas, 10% writes)', fontsize=16)
    plt.xticks(np.arange(len(percentiles)) + 0.2, percentiles)
    plt.legend()
    plt.grid(True, alpha=0.3, axis='y')
    plt.tight_layout()
    
    output_file = os.path.join(output_dir, 'latency_comparison.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Saved: {output_file}")
    plt.close()


def plot_throughput_breakdown(results, output_dir):
    """
    Additional plot: Get vs Put throughput breakdown
    """
    plt.figure(figsize=(12, 6))
    
    for version_name, version_data in sorted(results.items()):
        # Filter for 5 replicas
        filtered = [d for d in version_data if d.get('num_replicas') == 5]
        filtered.sort(key=lambda x: x.get('write_ratio', 0))
        
        write_ratios = [d['write_ratio'] for d in filtered]
        get_throughput = [d.get('get_throughput_ops_per_sec', 0) for d in filtered]
        put_throughput = [d.get('put_throughput_ops_per_sec', 0) for d in filtered]
        
        plt.plot(write_ratios, get_throughput, marker='o', label=f'{version_name} (Get)', linestyle='--')
        plt.plot(write_ratios, put_throughput, marker='s', label=f'{version_name} (Put)', linestyle='-')
    
    plt.xlabel('% write ratio', fontsize=14)
    plt.ylabel('Throughput (ops/sec)', fontsize=14)
    plt.title('Get vs Put Throughput (5 replicas)', fontsize=16)
    plt.grid(True, alpha=0.3)
    plt.legend(loc='best', fontsize=10)
    plt.tight_layout()
    
    output_file = os.path.join(output_dir, 'throughput_breakdown.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Saved: {output_file}")
    plt.close()


def generate_summary_table(results, output_dir):
    """
    Generate a summary table of key metrics
    """
    summary_file = os.path.join(output_dir, 'summary_table.txt')
    
    with open(summary_file, 'w') as f:
        f.write("=" * 100 + "\n")
        f.write("Raft Benchmark Results Summary\n")
        f.write("=" * 100 + "\n\n")
        
        for version_name, version_data in sorted(results.items()):
            f.write(f"\n{version_name}:\n")
            f.write("-" * 100 + "\n")
            f.write(f"{'Replicas':<10} {'Write%':<10} {'Throughput':<15} {'Avg Lat':<12} "
                   f"{'P95 Lat':<12} {'P99 Lat':<12} {'Success%':<12}\n")
            f.write("-" * 100 + "\n")
            
            for data in sorted(version_data, key=lambda x: (x.get('num_replicas', 0), x.get('write_ratio', 0))):
                replicas = data.get('num_replicas', 0)
                write_ratio = data.get('write_ratio', 0)
                throughput = data.get('throughput_ops_per_sec', 0)
                avg_lat = data.get('avg_latency_ms', 0)
                p95_lat = data.get('p95_latency_ms', 0)
                p99_lat = data.get('p99_latency_ms', 0)
                success_rate = data.get('success_rate', 0)
                
                f.write(f"{replicas:<10} {write_ratio:<10} {throughput:<15.2f} {avg_lat:<12.2f} "
                       f"{p95_lat:<12.2f} {p99_lat:<12.2f} {success_rate:<12.2f}\n")
            f.write("\n")
    
    print(f"Saved: {summary_file}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 plot_raft_results.py <benchmark_results_dir>")
        sys.exit(1)
    
    results_dir = sys.argv[1]
    
    if not os.path.exists(results_dir):
        print(f"Error: Directory not found: {results_dir}")
        sys.exit(1)
    
    print(f"Loading benchmark results from: {results_dir}")
    results = load_benchmark_results(results_dir)
    
    if not results:
        print("Error: No benchmark results found!")
        sys.exit(1)
    
    print(f"Found results for versions: {', '.join(results.keys())}")
    
    # Create output directory for plots
    output_dir = os.path.join(results_dir, "plots")
    os.makedirs(output_dir, exist_ok=True)
    
    print("\nGenerating plots...")
    
    # Generate all plots
    plot_throughput_vs_write_ratio(results, output_dir)
    plot_scalability_replicas(results, output_dir)
    plot_latency_comparison(results, output_dir)
    plot_throughput_breakdown(results, output_dir)
    generate_summary_table(results, output_dir)
    
    print(f"\nAll plots saved to: {output_dir}")
    print("\nGenerated files:")
    print("  - figure5_throughput_vs_write_ratio.png")
    print("  - figure7_scalability_replicas.png")
    print("  - latency_comparison.png")
    print("  - throughput_breakdown.png")
    print("  - summary_table.txt")


if __name__ == '__main__':
    main()