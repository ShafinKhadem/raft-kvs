#!/usr/bin/env python3
"""
Raft KVS Performance Benchmark Results Visualization Script
Generate professional performance charts similar to those in academic papers
"""

import os
import sys
import json
import glob
import argparse
from datetime import datetime
from typing import List, Dict, Any

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from pathlib import Path

# Font configuration
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial']
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['figure.dpi'] = 100
plt.rcParams['savefig.dpi'] = 300

# Color scheme - using colors commonly used in academic papers
COLORS = {
    'primary': '#2E86AB',
    'secondary': '#A23B72',
    'accent1': '#F18F01',
    'accent2': '#C73E1D',
    'accent3': '#6A994E',
    'grid': '#CCCCCC',
    'text': '#333333'
}

class BenchmarkPlotter:
    """Benchmark result plotting class"""
    
    def __init__(self, results_dir: str, output_dir: str = None):
        self.results_dir = Path(results_dir)
        self.output_dir = Path(output_dir) if output_dir else self.results_dir / "plots"
        self.output_dir.mkdir(exist_ok=True)
        
        self.throughput_data = []
        self.load_data()
    
    def load_data(self):
        """Load all JSON result files"""
        json_files = glob.glob(str(self.results_dir / "throughput_*.json"))
        
        for json_file in json_files:
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                    self.throughput_data.append(data)
            except Exception as e:
                print(f"Warning: Unable to load {json_file}: {e}")
        
        print(f"Loaded {len(self.throughput_data)} benchmark results")
    
    def plot_throughput_vs_clients(self):
        """Plot throughput vs number of clients"""
        # Filter data by fixed number of servers
        data_by_servers = {}
        for d in self.throughput_data:
            servers = d.get('num_servers', 5)
            # Skip records with 5 servers
            if servers == 5:
                continue
            if servers not in data_by_servers:
                data_by_servers[servers] = []
            data_by_servers[servers].append(d)
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        for servers in sorted(data_by_servers.keys()):
            data_list = data_by_servers[servers]
            # Sort by number of clients
            data_list.sort(key=lambda x: x['num_clients'])
            
            clients = [d['num_clients'] for d in data_list]
            throughput = [d['throughput_ops_per_sec'] for d in data_list]
            
            if len(clients) > 0:
                ax.plot(clients, throughput, marker='o', linewidth=2, 
                       markersize=8, label=f'{servers} Servers')
        
        ax.set_xlabel('Number of Clients', fontsize=12, fontweight='bold')
        ax.set_ylabel('Throughput (ops/sec)', fontsize=12, fontweight='bold')
        ax.set_title('Throughput vs Number of Clients', fontsize=14, fontweight='bold', pad=20)
        ax.grid(True, alpha=0.3, linestyle='--')
        ax.legend(fontsize=10, framealpha=0.9)
        
        plt.tight_layout()
        output_file = self.output_dir / "throughput_vs_clients.png"
        plt.savefig(output_file, bbox_inches='tight')
        print(f"✓ Generated: {output_file}")
        plt.close()
    
    def plot_latency_vs_clients(self):
        """Plot latency vs number of clients (including P50, P95, P99)"""
        # Use data with fixed number of servers (e.g., 4)
        data_list = [d for d in self.throughput_data if d.get('num_servers') == 4]
        data_list.sort(key=lambda x: x['num_clients'])
        
        if not data_list:
            print("Warning: No data found for servers=4")
            return
        
        clients = [d['num_clients'] for d in data_list]
        avg_latency = [d.get('avg_latency_ms', 0) for d in data_list]
        p50_latency = [d.get('p50_latency_ms', 0) for d in data_list]
        p95_latency = [d.get('p95_latency_ms', 0) for d in data_list]
        p99_latency = [d.get('p99_latency_ms', 0) for d in data_list]
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        ax.plot(clients, avg_latency, marker='s', linewidth=2, markersize=8, 
               label='Avg Latency', color=COLORS['primary'])
        ax.plot(clients, p50_latency, marker='o', linewidth=2, markersize=8, 
               label='P50 Latency', color=COLORS['accent3'])
        ax.plot(clients, p95_latency, marker='^', linewidth=2, markersize=8, 
               label='P95 Latency', color=COLORS['accent1'])
        ax.plot(clients, p99_latency, marker='d', linewidth=2, markersize=8, 
               label='P99 Latency', color=COLORS['accent2'])
        
        ax.set_xlabel('Number of Clients', fontsize=12, fontweight='bold')
        ax.set_ylabel('Latency (ms)', fontsize=12, fontweight='bold')
        ax.set_title('Latency vs Number of Clients (5 Servers)', fontsize=14, fontweight='bold', pad=20)
        ax.grid(True, alpha=0.3, linestyle='--')
        ax.legend(fontsize=10, framealpha=0.9)
        
        plt.tight_layout()
        output_file = self.output_dir / "latency_vs_clients.png"
        plt.savefig(output_file, bbox_inches='tight')
        print(f"✓ Generated: {output_file}")
        plt.close()
    
    def plot_throughput_vs_servers(self):
        """Plot throughput vs number of servers"""
        # Filter data by fixed number of clients
        data_by_clients = {}
        for d in self.throughput_data:
            # Skip records with 5 servers
            if d.get('num_servers', 5) == 5:
                continue
            clients = d.get('num_clients', 5)
            if clients not in data_by_clients:
                data_by_clients[clients] = []
            data_by_clients[clients].append(d)
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        for clients in sorted(data_by_clients.keys()):
            data_list = data_by_clients[clients]
            # Sort by number of servers
            data_list.sort(key=lambda x: x['num_servers'])
            
            servers = [d['num_servers'] for d in data_list]
            throughput = [d['throughput_ops_per_sec'] for d in data_list]
            
            if len(servers) > 1:  # Only show with multiple data points
                ax.plot(servers, throughput, marker='o', linewidth=2, 
                       markersize=8, label=f'{clients} Clients')
        
        ax.set_xlabel('Number of Servers', fontsize=12, fontweight='bold')
        ax.set_ylabel('Throughput (ops/sec)', fontsize=12, fontweight='bold')
        ax.set_title('Throughput vs Number of Servers', fontsize=14, fontweight='bold', pad=20)
        ax.grid(True, alpha=0.3, linestyle='--')
        ax.legend(fontsize=10, framealpha=0.9)
        
        plt.tight_layout()
        output_file = self.output_dir / "throughput_vs_servers.png"
        plt.savefig(output_file, bbox_inches='tight')
        print(f"✓ Generated: {output_file}")
        plt.close()
    
    def plot_success_rate(self):
        """Plot success rate bar chart"""
        data_list = [d for d in self.throughput_data if d.get('num_servers') == 5]
        data_list.sort(key=lambda x: x['num_clients'])
        
        if not data_list:
            return
        
        clients = [f"{d['num_clients']} Clients" for d in data_list]
        success_rate = [d.get('success_rate', 0) for d in data_list]
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        bars = ax.bar(clients, success_rate, color=COLORS['accent3'], 
                     alpha=0.8, edgecolor='black', linewidth=1.5)
        
        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{height:.1f}%', ha='center', va='bottom', fontweight='bold')
        
        ax.set_ylabel('Success Rate (%)', fontsize=12, fontweight='bold')
        ax.set_title('Operation Success Rate (5 Servers)', fontsize=14, fontweight='bold', pad=20)
        ax.set_ylim([0, 105])
        ax.grid(True, alpha=0.3, linestyle='--', axis='y')
        
        plt.tight_layout()
        output_file = self.output_dir / "success_rate.png"
        plt.savefig(output_file, bbox_inches='tight')
        print(f"✓ Generated: {output_file}")
        plt.close()
    
    def plot_operation_breakdown(self):
        """Plot operation type breakdown (OK, Maybe, VersionErr)"""
        data_list = [d for d in self.throughput_data if d.get('num_servers') == 5]
        data_list.sort(key=lambda x: x['num_clients'])
        
        if not data_list:
            return
        
        clients = [d['num_clients'] for d in data_list]
        ok_counts = [d.get('ok_count', 0) for d in data_list]
        maybe_counts = [d.get('maybe_count', 0) for d in data_list]
        version_err_counts = [d.get('version_err_count', 0) for d in data_list]
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        x = np.arange(len(clients))
        width = 0.25
        
        ax.bar(x - width, ok_counts, width, label='OK', 
              color=COLORS['accent3'], edgecolor='black', linewidth=1)
        ax.bar(x, maybe_counts, width, label='Maybe', 
              color=COLORS['accent1'], edgecolor='black', linewidth=1)
        ax.bar(x + width, version_err_counts, width, label='VersionErr', 
              color=COLORS['accent2'], edgecolor='black', linewidth=1)
        
        ax.set_xlabel('Number of Clients', fontsize=12, fontweight='bold')
        ax.set_ylabel('Number of Operations', fontsize=12, fontweight='bold')
        ax.set_title('Operation Type Distribution (5 Servers)', fontsize=14, fontweight='bold', pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels(clients)
        ax.legend(fontsize=10, framealpha=0.9)
        ax.grid(True, alpha=0.3, linestyle='--', axis='y')
        
        plt.tight_layout()
        output_file = self.output_dir / "operation_breakdown.png"
        plt.savefig(output_file, bbox_inches='tight')
        print(f"✓ Generated: {output_file}")
        plt.close()
    
    def plot_latency_cdf(self):
        """Plot latency CDF (Cumulative Distribution Function)"""
        # This requires raw latency data, here we approximate with percentile data
        data_list = [d for d in self.throughput_data if d.get('num_servers') == 4]
        
        if not data_list:
            return
        
        fig, ax = plt.subplots(figsize=(10, 6))
        
        for d in data_list:
            clients = d['num_clients']
            # Use percentile data to build approximate CDF
            latencies = [
                d.get('p50_latency_ms', 0),
                d.get('p95_latency_ms', 0),
                d.get('p99_latency_ms', 0)
            ]
            percentiles = [50, 95, 99]
            
            if max(latencies) > 0:
                ax.plot(latencies, [p/100 for p in percentiles], 
                       marker='o', linewidth=2, markersize=6,
                       label=f'{clients} Clients')
        
        ax.set_xlabel('Latency (ms)', fontsize=12, fontweight='bold')
        ax.set_ylabel('Cumulative Probability', fontsize=12, fontweight='bold')
        ax.set_title('Latency Cumulative Distribution Function (CDF)', fontsize=14, fontweight='bold', pad=20)
        ax.grid(True, alpha=0.3, linestyle='--')
        ax.legend(fontsize=10, framealpha=0.9)
        
        plt.tight_layout()
        output_file = self.output_dir / "latency_cdf.png"
        plt.savefig(output_file, bbox_inches='tight')
        print(f"✓ Generated: {output_file}")
        plt.close()
    
    def plot_scalability_heatmap(self):
        """Plot scalability heatmap (clients x servers)"""
        # Build heatmap data matrix, skip records with 5 servers
        filtered_data = [d for d in self.throughput_data if d.get('num_servers', 5) != 5]
        client_counts = sorted(set(d['num_clients'] for d in filtered_data))
        server_counts = sorted(set(d['num_servers'] for d in filtered_data))
        
        if len(client_counts) < 2 or len(server_counts) < 2:
            print("Warning: Heatmap requires multiple client and server configurations")
            return
        
        # Create matrix
        matrix = np.zeros((len(client_counts), len(server_counts)))
        
        for d in filtered_data:
            try:
                i = client_counts.index(d['num_clients'])
                j = server_counts.index(d['num_servers'])
                matrix[i][j] = d['throughput_ops_per_sec']
            except (ValueError, KeyError):
                continue
        
        fig, ax = plt.subplots(figsize=(10, 8))
        
        im = ax.imshow(matrix, cmap='YlOrRd', aspect='auto')
        
        # Set ticks
        ax.set_xticks(np.arange(len(server_counts)))
        ax.set_yticks(np.arange(len(client_counts)))
        ax.set_xticklabels(server_counts)
        ax.set_yticklabels(client_counts)
        
        ax.set_xlabel('Number of Servers', fontsize=12, fontweight='bold')
        ax.set_ylabel('Number of Clients', fontsize=12, fontweight='bold')
        ax.set_title('Throughput Heatmap (ops/sec)', fontsize=14, fontweight='bold', pad=20)
        
        # Add colorbar
        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label('Throughput (ops/sec)', rotation=270, labelpad=20, fontweight='bold')
        
        # Add values to each cell
        for i in range(len(client_counts)):
            for j in range(len(server_counts)):
                if matrix[i, j] > 0:
                    text = ax.text(j, i, f'{matrix[i, j]:.0f}',
                                 ha="center", va="center", color="black", fontweight='bold')
        
        plt.tight_layout()
        output_file = self.output_dir / "scalability_heatmap.png"
        plt.savefig(output_file, bbox_inches='tight')
        print(f"✓ Generated: {output_file}")
        plt.close()
    
    def generate_summary_report(self):
        """Generate text summary report"""
        report_file = self.output_dir / "performance_summary.txt"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("Raft KVS Performance Benchmark Summary Report\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"Test Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Results Directory: {self.results_dir}\n")
            f.write(f"Total Tests: {len(self.throughput_data)}\n\n")
            
            if self.throughput_data:
                # Calculate statistics
                throughputs = [d['throughput_ops_per_sec'] for d in self.throughput_data]
                latencies = [d.get('avg_latency_ms', 0) for d in self.throughput_data]
                success_rates = [d.get('success_rate', 0) for d in self.throughput_data]
                
                f.write("-" * 80 + "\n")
                f.write("Overall Statistics\n")
                f.write("-" * 80 + "\n")
                f.write(f"Throughput Range: {min(throughputs):.2f} - {max(throughputs):.2f} ops/sec\n")
                f.write(f"Average Throughput: {np.mean(throughputs):.2f} ops/sec\n")
                f.write(f"Latency Range: {min(latencies):.2f} - {max(latencies):.2f} ms\n")
                f.write(f"Average Latency: {np.mean(latencies):.2f} ms\n")
                f.write(f"Average Success Rate: {np.mean(success_rates):.2f}%\n\n")
                
                f.write("-" * 80 + "\n")
                f.write("Detailed Results\n")
                f.write("-" * 80 + "\n")
                f.write(f"{'Clients':<8} {'Servers':<8} {'Throughput':<15} {'Avg Latency':<12} {'Success Rate':<10}\n")
                f.write("-" * 80 + "\n")
                
                for d in sorted(self.throughput_data, 
                               key=lambda x: (x['num_clients'], x['num_servers'])):
                    f.write(f"{d['num_clients']:<8} {d['num_servers']:<8} "
                           f"{d['throughput_ops_per_sec']:>10.2f} ops/s "
                           f"{d.get('avg_latency_ms', 0):>8.2f} ms "
                           f"{d.get('success_rate', 0):>7.2f}%\n")
                
                f.write("\n")
                
                # Best configurations
                best_throughput = max(self.throughput_data, 
                                    key=lambda x: x['throughput_ops_per_sec'])
                best_latency = min(self.throughput_data, 
                                 key=lambda x: x.get('avg_latency_ms', float('inf')))
                
                f.write("-" * 80 + "\n")
                f.write("Best Configurations\n")
                f.write("-" * 80 + "\n")
                f.write(f"Highest Throughput: {best_throughput['throughput_ops_per_sec']:.2f} ops/sec\n")
                f.write(f"  Configuration: {best_throughput['num_clients']} clients, "
                       f"{best_throughput['num_servers']} servers\n\n")
                f.write(f"Lowest Latency: {best_latency.get('avg_latency_ms', 0):.2f} ms\n")
                f.write(f"  Configuration: {best_latency['num_clients']} clients, "
                       f"{best_latency['num_servers']} servers\n\n")
            
            f.write("=" * 80 + "\n")
        
        print(f"✓ Generated summary report: {report_file}")
    
    def generate_all_plots(self):
        """Generate all plots"""
        print("\nStarting performance chart generation...")
        print("=" * 60)
        
        self.plot_throughput_vs_clients()
        self.plot_latency_vs_clients()
        self.plot_throughput_vs_servers()
        self.plot_success_rate()
        self.plot_operation_breakdown()
        self.plot_latency_cdf()
        self.plot_scalability_heatmap()
        self.generate_summary_report()
        
        print("=" * 60)
        print(f"\n✓ All charts generated to: {self.output_dir}")
        print(f"\nYou can view the charts with:")
        print(f"  open {self.output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description='Raft KVS Performance Benchmark Results Visualization Tool'
    )
    parser.add_argument(
        'results_dir',
        help='Path to benchmark results directory'
    )
    parser.add_argument(
        '-o', '--output',
        help='Chart output directory (default: plots subdirectory in results directory)',
        default=None
    )
    
    args = parser.parse_args()
    
    if not os.path.exists(args.results_dir):
        print(f"Error: Results directory does not exist: {args.results_dir}")
        sys.exit(1)
    
    plotter = BenchmarkPlotter(args.results_dir, args.output)
    plotter.generate_all_plots()


if __name__ == '__main__':
    main()
