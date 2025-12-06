import json

import matplotlib.pyplot as plt

# -------------------------------------------------------------------
# Define the benchmark result files for each version of the system.
# Replace these lists with the actual file paths for each version.
# -------------------------------------------------------------------

version_files = {
    "Base": [
        "./benchmark_results/base_20251205_211659/replicas5_writes0_clients5.json",
        "./benchmark_results/base_20251205_211659/replicas5_writes0_clients10.json",
        "./benchmark_results/base_20251205_211659/replicas5_writes0_clients20.json",
        "./benchmark_results/base_20251205_211659/replicas5_writes0_clients50.json",
        "./benchmark_results/base_20251205_211659/replicas5_writes0_clients100.json",
        "./benchmark_results/base_20251205_211659/replicas5_writes0_clients200.json",
        "./benchmark_results/base_20251205_211659/replicas5_writes0_clients500.json",
    ],
    "ALR-leader-only": [
        "./benchmark_results/ALR-leader-only_20251205_210913/replicas5_writes0_clients5.json",
        "./benchmark_results/ALR-leader-only_20251205_210913/replicas5_writes0_clients10.json",
        "./benchmark_results/ALR-leader-only_20251205_210913/replicas5_writes0_clients20.json",
        "./benchmark_results/ALR-leader-only_20251205_210913/replicas5_writes0_clients50.json",
        "./benchmark_results/ALR-leader-only_20251205_210913/replicas5_writes0_clients100.json",
        "./benchmark_results/ALR-leader-only_20251205_210913/replicas5_writes0_clients200.json",
        "./benchmark_results/ALR-leader-only_20251205_210913/replicas5_writes0_clients500.json",
        "./benchmark_results/ALR-leader-only_20251205_210913/replicas5_writes0_clients1000.json",
        "./benchmark_results/ALR-leader-only_20251205_210913/replicas5_writes0_clients2000.json",
        "./benchmark_results/ALR-leader-only_20251205_210913/replicas5_writes0_clients5000.json",
    ],
    "ALR": [
        "./benchmark_results/ALR_20251205_210249/replicas5_writes0_clients5.json",
        "./benchmark_results/ALR_20251205_210249/replicas5_writes0_clients10.json",
        "./benchmark_results/ALR_20251205_210249/replicas5_writes0_clients20.json",
        "./benchmark_results/ALR_20251205_210249/replicas5_writes0_clients50.json",
        "./benchmark_results/ALR_20251205_210249/replicas5_writes0_clients100.json",
        "./benchmark_results/ALR_20251205_210249/replicas5_writes0_clients200.json",
        "./benchmark_results/ALR_20251205_210249/replicas5_writes0_clients500.json",
        "./benchmark_results/ALR_20251205_210249/replicas5_writes0_clients1000.json",
        "./benchmark_results/ALR_20251205_210249/replicas5_writes0_clients2000.json",
        "./benchmark_results/ALR_20251205_210249/replicas5_writes0_clients4000.json",
        "./benchmark_results/ALR_20251205_210249/replicas5_writes0_clients5000.json",
    ],
}


# -------------------------------------------------------------------
# Load all data
# -------------------------------------------------------------------
def load_benchmarks(file_list):
    clients = []
    throughput = []
    for f in file_list:
        with open(f, "r") as infile:
            data = json.load(infile)
            clients.append(data["num_clients"])
            throughput.append(data["throughput_ops_per_sec"])
    return clients, throughput


# -------------------------------------------------------------------
# Plotting
# -------------------------------------------------------------------
plt.figure(figsize=(8, 6))

for version_name, file_list in version_files.items():
    clients, thr = load_benchmarks(file_list)
    # Sort by client count to ensure ordered plotting
    sorted_pairs = sorted(zip(clients, thr))
    clients_sorted, thr_sorted = zip(*sorted_pairs)

    plt.plot(clients_sorted, thr_sorted, marker="o", label=version_name)

plt.xlabel("Number of Concurrent Clients")
plt.ylabel("Throughput (ops/sec)")
plt.title("Read Throughput vs Number of Concurrent Clients (5 replicas, no writes)")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.show()
