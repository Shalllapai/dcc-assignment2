import argparse
import sys
import time
import uuid
import grpc

import counter_pb2
import counter_pb2_grpc


class CounterClient:
    def __init__(self, target: str = "localhost:50051"):
        self.channel = grpc.insecure_channel(target)
        self.stub = counter_pb2_grpc.CounterStub(self.channel)

    def incr(self, counter_id: str, delta: int = 1, key: str = None) -> counter_pb2.IncrementReply:
        if key is None:
            key = str(uuid.uuid4())

        request = counter_pb2.IncrementRequest(
            counter_id=counter_id,
            delta=delta,
            idempotency_key=key
        )

        backoff_delays = [0.2, 0.4, 0.8]
        last_error = None

        for delay in [0.0] + backoff_delays:
            if delay > 0:
                time.sleep(delay)

            try:
                reply = self.stub.Increment(request, timeout=2.0)
                return reply
            except grpc.RpcError as e:
                code = e.code()
                if code in (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE):
                    last_error = e
                    continue
                raise e

        raise last_error

    def get(self, counter_id: str) -> counter_pb2.GetReply:
        request = counter_pb2.GetRequest(counter_id=counter_id)
        return self.stub.Get(request, timeout=2.0)

    def close(self):
        self.channel.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="localhost:50051")
    subparsers = parser.add_subparsers(dest="command", required=True)

    incr_parser = subparsers.add_parser("incr")
    incr_parser.add_argument("counter_id", type=str)
    incr_parser.add_argument("--by", type=int, default=1)
    incr_parser.add_argument("--key", type=str, default=None)

    get_parser = subparsers.add_parser("get")
    get_parser.add_argument("counter_id", type=str)

    args = parser.parse_args()
    client = CounterClient(target=args.target)

    try:
        if args.command == "incr":
            reply = client.incr(args.counter_id, delta=args.by, key=args.key)
            dup_str = "yes" if reply.was_duplicate else "no"
            print(f"OK committed value={reply.new_value} (duplicate: {dup_str})")
        elif args.command == "get":
            reply = client.get(args.counter_id)
            if reply.found:
                print(f"value={reply.value}")
            else:
                print("counter not found (value=0)")
    finally:
        client.close()


if __name__ == "__main__":
    main()