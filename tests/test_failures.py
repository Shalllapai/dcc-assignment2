import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import socket
import subprocess
import threading
import time
import pytest

from client import CounterClient, SingleReplicaClient


def get_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def start_server(port, node_id="replica", delay_ms=0, fault="none"):
    cmd = [
        sys.executable,
        "server.py",
        "--node-id",
        node_id,
        "--port",
        str(port),
        "--delay-ms",
        str(delay_ms),
        "--fault",
        fault,
    ]
    proc = subprocess.Popen(cmd)
    time.sleep(0.3)
    return proc


def test_failure_replica_crash_mid_request():
    ports = [get_free_port() for _ in range(3)]
    p1 = start_server(ports[0], "replica-A")
    p2 = start_server(ports[1], "replica-B")
    p3 = start_server(ports[2], "replica-C")

    targets = [f"localhost:{p}" for p in ports]
    client = CounterClient(targets=targets)

    def killer():
        time.sleep(0.05)
        p3.kill()
        p3.wait()

    t = threading.Thread(target=killer)
    t.start()

    res = client.incr("crash_mid_test", delta=10)
    t.join()

    assert res["value"] == 10
    assert res["acked"] >= 2

    client.close()
    p1.terminate()
    p2.terminate()
    p1.wait()
    p2.wait()


def test_failure_request_duplication():
    ports = [get_free_port() for _ in range(3)]
    procs = [
        start_server(ports[0], "replica-A"),
        start_server(ports[1], "replica-B"),
        start_server(ports[2], "replica-C"),
    ]
    targets = [f"localhost:{p}" for p in ports]
    client = CounterClient(targets=targets)

    res1 = client.incr("dup_test_counter", delta=15, key="same-operation-key")
    assert res1["value"] == 15
    assert res1["was_duplicate"] is False

    res2 = client.incr("dup_test_counter", delta=15, key="same-operation-key")
    assert res2["value"] == 15
    assert res2["was_duplicate"] is True

    get_res = client.get("dup_test_counter")
    assert get_res.value == 15

    client.close()
    for p in procs:
        p.terminate()
        p.wait()


def test_failure_induced_timeout_with_retry():
    port = get_free_port()
    proc = start_server(port, node_id="replica-delay", fault="delay-once")
    client = CounterClient(targets=[f"localhost:{port}"])

    res = client.incr("delayed_counter", delta=42, key="retry-timeout-key")

    assert res["value"] == 42

    time.sleep(1.0)
    get_res = client.get("delayed_counter")
    assert get_res.value == 42

    client.close()
    proc.terminate()
    proc.wait()