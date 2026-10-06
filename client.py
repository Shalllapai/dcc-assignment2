import argparse
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock


class QuorumError(Exception):
    pass


class SingleReplicaClient:
    def __init__(self, target: str):
        self.target = target
        self.channel = grpc.insecure_channel(target)
        self.stub = counter_pb2_grpc.CounterStub(self.channel)

    def send_incr(self, request: counter_pb2.IncrementRequest) -> counter_pb2.IncrementReply:
        backoff_delays = [0.2, 0.4, 0.8]
        last_error = None

        for delay in [0.0] + backoff_delays:
            if delay > 0:
                time.sleep(delay)
            try:
                return self.stub.Increment(request, timeout=2.0)
            except grpc.RpcError as e:
                code = e.code()
                if code in (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE):
                    last_error = e
                    continue
                raise e
        raise last_error

    def send_get(self, request: counter_pb2.GetRequest) -> counter_pb2.GetReply:
        return self.stub.Get(request, timeout=2.0)

    def close(self):
        self.channel.close()


class CounterClient:
    def __init__(self, targets: Optional[List[str]] = None, client_id: str = "client-1"):
        if targets is None:
            self.targets = ["localhost:50051", "localhost:50052", "localhost:50053"]
        elif isinstance(targets, str):
            self.targets = [t.strip() for t in targets.split(",")]
        else:
            self.targets = list(targets)

        self.client_id = client_id
        self.clock = LamportClock()
        self.replicas = [SingleReplicaClient(t) for t in self.targets]

    def incr(self, counter_id: str, delta: int = 1, key: str = None):
        if key is None:
            key = str(uuid.uuid4())

        send_l = self.clock.tick()
        request = counter_pb2.IncrementRequest(
            counter_id=counter_id,
            delta=delta,
            idempotency_key=key,
            lamport_time=send_l
        )

        responses = []
        with ThreadPoolExecutor(max_workers=len(self.replicas)) as executor:
            future_to_replica = {
                executor.submit(replica.send_incr, request): replica
                for replica in self.replicas
            }
            for future in as_completed(future_to_replica):
                try:
                    reply = future.result()
                    responses.append(reply)
                    self.clock.update(reply.lamport_time)
                except Exception:
                    pass

        majority = (len(self.replicas) // 2) + 1
        acked_count = len(responses)

        if acked_count < majority:
            raise QuorumError(f"Quorum not reached: {acked_count}/{len(self.replicas)} replicas acknowledged (needed {majority})")

        sample_reply = responses[0]
        return {
            "value": sample_reply.new_value,
            "acked": acked_count,
            "total": len(self.replicas),
            "was_duplicate": sample_reply.was_duplicate
        }

    def get(self, counter_id: str):
        send_l = self.clock.tick()
        request = counter_pb2.GetRequest(counter_id=counter_id, lamport_time=send_l)

        for replica in self.replicas:
            try:
                reply = replica.send_get(request)
                self.clock.update(reply.lamport_time)
                return reply
            except Exception:
                continue

        raise ConnectionError("No replicas available to serve Get request")

    def close(self):
        for replica in self.replicas:
            replica.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--targets", default="localhost:50051,localhost:50052,localhost:50053")
    parser.add_argument("--client-id", default="client-1")
    subparsers = parser.add_subparsers(dest="command", required=True)

    incr_parser = subparsers.add_parser("incr")
    incr_parser.add_argument("counter_id", type=str)
    incr_parser.add_argument("--by", type=int, default=1)
    incr_parser.add_argument("--key", type=str, default=None)

    get_parser = subparsers.add_parser("get")
    get_parser.add_argument("counter_id", type=str)

    args = parser.parse_args()
    client = CounterClient(targets=args.targets, client_id=args.client_id)

    try:
        if args.command == "incr":
            res = client.incr(args.counter_id, delta=args.by, key=args.key)
            dup_str = "yes" if res["was_duplicate"] else "no"
            print(f"OK committed value={res['value']} (replicas acked: {res['acked']}/{res['total']}, duplicate: {dup_str})")
        elif args.command == "get":
            reply = client.get(args.counter_id)
            if reply.found:
                print(f"value={reply.value}")
            else:
                print("counter not found (value=0)")
    except QuorumError as e:
        print(f"FAILED: {e}")
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()