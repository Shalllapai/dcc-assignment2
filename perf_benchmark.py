import math
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from client import CounterClient


def get_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def start_server(port, node_id="replica"):
    cmd = [
        sys.executable,
        "server.py",
        "--node-id",
        node_id,
        "--port",
        str(port),
    ]
    proc = subprocess.Popen(cmd)
    time.sleep(0.3)
    return proc


def run_benchmark_config(targets, num_clients=1, total_requests=2000):
    latencies = []
    lock = threading.Lock()
    reqs_per_client = total_requests // num_clients

    def worker(worker_id):
        client = CounterClient(targets=targets, client_id=f"bench-c-{worker_id}")
        local_latencies = []
        for i in range(reqs_per_client):
            start = time.perf_counter()
            client.incr("bench_counter", delta=1)
            duration_ms = (time.perf_counter() - start) * 1000.0
            local_latencies.append(duration_ms)
        client.close()
        with lock:
            latencies.extend(local_latencies)

    threads = []
    for cid in range(num_clients):
        t = threading.Thread(target=worker, args=(cid,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    latencies.sort()
    n = len(latencies)
    median = latencies[n // 2]
    p95_idx = math.ceil(0.95 * n) - 1
    p95 = latencies[max(0, min(p95_idx, n - 1))]

    return median, p95, n


def main():
    print("Starting Benchmark across 4 configurations (2000 requests each)...")

    # 1 & 2: Single replica
    p_single = get_free_port()
    proc_single = start_server(p_single, "single-replica")
    single_target = [f"localhost:{p_single}"]

    print("Running: Single replica, 1 client...")
    med1, p95_1, n1 = run_benchmark_config(single_target, num_clients=1, total_requests=2000)

    print("Running: Single replica, 16 clients...")
    med2, p95_2, n2 = run_benchmark_config(single_target, num_clients=16, total_requests=2000)

    proc_single.terminate()
    proc_single.wait()

    # 3 & 4: Quorum (3 replicas)
    ports_q = [get_free_port() for _ in range(3)]
    procs_q = [
        start_server(ports_q[0], "rep-A"),
        start_server(ports_q[1], "rep-B"),
        start_server(ports_q[2], "rep-C"),
    ]
    quorum_targets = [f"localhost:{p}" for p in ports_q]

    print("Running: Quorum (3 replicas), 1 client...")
    med3, p95_3, n3 = run_benchmark_config(quorum_targets, num_clients=1, total_requests=2000)

    print("Running: Quorum (3 replicas), 16 clients...")
    med4, p95_4, n4 = run_benchmark_config(quorum_targets, num_clients=16, total_requests=2000)

    for p in procs_q:
        p.terminate()
        p.wait()

    print("\n" + "=" * 65)
    print(f"{'Configuration':<30} | {'Median (ms)':<11} | {'p95 (ms)':<8} | {'Requests'}")
    print("-" * 65)
    print(f"{'Single replica, 1 client':<30} | {med1:<11.2f} | {p95_1:<8.2f} | {n1}")
    print(f"{'Single replica, 16 clients':<30} | {med2:<11.2f} | {p95_2:<8.2f} | {n2}")
    print(f"{'Quorum (3 replicas), 1 client':<30} | {med3:<11.2f} | {p95_3:<8.2f} | {n3}")
    print(f"{'Quorum (3 replicas), 16 clients':<30} | {med4:<11.2f} | {p95_4:<8.2f} | {n4}")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    main()