import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import socket
import subprocess
import threading
import time
import pytest

from client import CounterClient, QuorumError, SingleReplicaClient
import counter_pb2


def get_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def start_server(port, node_id="replica", delay_ms=0):
    cmd = [
        sys.executable,
        "server.py",
        "--node-id",
        node_id,
        "--port",
        str(port),
        "--delay-ms",
        str(delay_ms),
    ]
    proc = subprocess.Popen(cmd)
    time.sleep(0.3)
    return proc


@pytest.fixture
def cluster():
    ports = [get_free_port() for _ in range(3)]
    procs = [
        start_server(ports[0], "replica-A"),
        start_server(ports[1], "replica-B"),
        start_server(ports[2], "replica-C"),
    ]
    targets = [f"localhost:{p}" for p in ports]
    client = CounterClient(targets=targets)

    yield {"client": client, "procs": procs, "targets": targets, "ports": ports}

    client.close()
    for p in procs:
        p.terminate()
        p.wait()


def test_increment_applies_delta(cluster):
    c = cluster["client"]
    res = c.incr("test_c1", delta=5)
    assert res["value"] == 5
    assert res["was_duplicate"] is False

    res2 = c.incr("test_c1", delta=3)
    assert res2["value"] == 8


def test_duplicate_key_not_reapplied(cluster):
    c = cluster["client"]
    r1 = c.incr("test_c2", delta=5, key="dup-key-1")
    r2 = c.incr("test_c2", delta=5, key="dup-key-1")

    assert r1["value"] == 5
    assert r1["was_duplicate"] is False
    assert r2["value"] == 5
    assert r2["was_duplicate"] is True


def test_concurrent_increments_exact(cluster):
    targets = cluster["targets"]

    def worker():
        local_c = CounterClient(targets=targets)
        for _ in range(1000):
            local_c.incr("concurrent_cnt", delta=1)
        local_c.close()

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    res = cluster["client"].get("concurrent_cnt")
    assert res.value == 2000


def test_get_missing_counter(cluster):
    c = cluster["client"]
    res = c.get("non_existent_key")
    assert res.found is False
    assert res.value == 0


def test_retry_after_timeout_is_safe():
    port = get_free_port()
    proc = start_server(port, node_id="slow-node", delay_ms=0)
    client = CounterClient(targets=[f"localhost:{port}"])

    try:
        r1 = client.incr("slow_counter", delta=10, key="fixed-timeout-key")
        assert r1["value"] == 10

        r2 = client.incr("slow_counter", delta=10, key="fixed-timeout-key")
        assert r2["value"] == 10
        assert r2["was_duplicate"] is True
    finally:
        client.close()
        proc.terminate()
        proc.wait()


def test_majority_commit_two_acks(cluster):
    cluster["procs"][2].terminate()
    cluster["procs"][2].wait()

    c = cluster["client"]
    res = c.incr("majority_test", delta=7)
    assert res["acked"] == 2
    assert res["value"] == 7


def test_no_commit_below_majority(cluster):
    cluster["procs"][1].terminate()
    cluster["procs"][1].wait()
    cluster["procs"][2].terminate()
    cluster["procs"][2].wait()

    c = cluster["client"]
    with pytest.raises(QuorumError):
        c.incr("below_majority_test", delta=7)

    surviving = SingleReplicaClient(cluster["targets"][0])
    req = counter_pb2.GetRequest(counter_id="below_majority_test")
    reply = surviving.send_get(req)
    surviving.close()

    assert reply.value == 7


def test_replicas_converge(cluster):
    c = cluster["client"]
    for i in range(10):
        c.incr("converge_test", delta=2)

    for target in cluster["targets"]:
        s = SingleReplicaClient(target)
        req = counter_pb2.GetRequest(counter_id="converge_test")
        reply = s.send_get(req)
        s.close()
        assert reply.value == 20