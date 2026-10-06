import argparse
import sys
import threading
import time
from concurrent import futures
import grpc

import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, node_id: str = "replica-A", delay_ms: int = 0, fault: str = "none"):
        self.node_id = node_id
        self.clock = LamportClock()
        self._lock = threading.Lock()
        self._values = {}
        self._seen = {}
        self._delay_ms = delay_ms
        self._fault = fault

    def Increment(self, request: counter_pb2.IncrementRequest, context):
        recv_l = self.clock.update(request.lamport_time)
        print(f"[{self.node_id}] RECV Increment(counter={request.counter_id}, delta={request.delta}) L={recv_l} (received L={request.lamport_time})", flush=True)

        if self._delay_ms > 0:
            time.sleep(self._delay_ms / 1000.0)

        with self._lock:
            if request.idempotency_key in self._seen:
                _, stored_val = self._seen[request.idempotency_key]
                send_l = self.clock.tick()
                print(f"[{self.node_id}] SEND IncrementReply(new_value={stored_val}) L={send_l}", flush=True)
                return counter_pb2.IncrementReply(
                    new_value=stored_val,
                    was_duplicate=True,
                    lamport_time=send_l
                )

            current_val = self._values.get(request.counter_id, 0)
            new_val = current_val + request.delta
            self._values[request.counter_id] = new_val
            self._seen[request.idempotency_key] = (request.counter_id, new_val)

            apply_l = self.clock.tick()
            print(f"[{self.node_id}] APPLY counter={request.counter_id} -> {new_val} L={apply_l}", flush=True)

            send_l = self.clock.tick()
            print(f"[{self.node_id}] SEND IncrementReply(new_value={new_val}) L={send_l}", flush=True)
            return counter_pb2.IncrementReply(
                new_value=new_val,
                was_duplicate=False,
                lamport_time=send_l
            )

    def Get(self, request: counter_pb2.GetRequest, context):
        recv_l = self.clock.update(request.lamport_time)
        print(f"[{self.node_id}] RECV Get(counter={request.counter_id}) L={recv_l} (received L={request.lamport_time})", flush=True)

        with self._lock:
            val = self._values.get(request.counter_id, 0)
            found = request.counter_id in self._values

            send_l = self.clock.tick()
            print(f"[{self.node_id}] SEND GetReply(value={val}) L={send_l}", flush=True)
            return counter_pb2.GetReply(
                value=val,
                found=found,
                lamport_time=send_l
            )


def serve(node_id: str = "replica-A", port: int = 50051, delay_ms: int = 0, fault: str = "none"):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    servicer = CounterServicer(node_id=node_id, delay_ms=delay_ms, fault=fault)
    counter_pb2_grpc.add_CounterServicer_to_server(servicer, server)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    print(f"[{node_id}] Server started on port {port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--node-id", type=str, default="replica-A")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--delay-ms", type=int, default=0)
    parser.add_argument("--fault", type=str, default="none")
    args = parser.parse_args()

    serve(node_id=args.node_id, port=args.port, delay_ms=args.delay_ms, fault=args.fault)