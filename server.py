import argparse
import sys
import threading
import time
from concurrent import futures
import grpc

import counter_pb2
import counter_pb2_grpc


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, delay_ms: int = 0, fault: str = "none"):
        self._lock = threading.Lock()
        self._values = {}
        self._seen = {}
        self._delay_ms = delay_ms
        self._fault = fault

    def Increment(self, request: counter_pb2.IncrementRequest, context):
        if self._delay_ms > 0:
            time.sleep(self._delay_ms / 1000.0)

        with self._lock:
            if request.idempotency_key in self._seen:
                _, stored_val = self._seen[request.idempotency_key]
                return counter_pb2.IncrementReply(
                    new_value=stored_val,
                    was_duplicate=True
                )

            current_val = self._values.get(request.counter_id, 0)
            new_val = current_val + request.delta
            self._values[request.counter_id] = new_val
            self._seen[request.idempotency_key] = (request.counter_id, new_val)

            return counter_pb2.IncrementReply(
                new_value=new_val,
                was_duplicate=False
            )

    def Get(self, request: counter_pb2.GetRequest, context):
        with self._lock:
            if request.counter_id in self._values:
                return counter_pb2.GetReply(
                    value=self._values[request.counter_id],
                    found=True
                )
            return counter_pb2.GetReply(value=0, found=False)


def serve(port: int = 50051, delay_ms: int = 0, fault: str = "none"):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    servicer = CounterServicer(delay_ms=delay_ms, fault=fault)
    counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(f"Server started on port {port}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--delay-ms", type=int, default=0)
    parser.add_argument("--fault", type=str, default="none")
    args = parser.parse_args()

    serve(port=args.port, delay_ms=args.delay_ms, fault=args.fault)