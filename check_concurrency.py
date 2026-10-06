import threading
from client import CounterClient


def worker(counter_name: str, count: int):
    client = CounterClient("localhost:50051")
    for _ in range(count):
        client.incr(counter_name, delta=1)
    client.close()


def main():
    counter = "test_concurrent_counter"
    t1 = threading.Thread(target=worker, args=(counter, 1000))
    t2 = threading.Thread(target=worker, args=(counter, 1000))

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    client = CounterClient("localhost:50051")
    reply = client.get(counter)
    client.close()

    assert reply.value == 2000, f"Expected 2000, got {reply.value}"
    print(f"SUCCESS: Exact value {reply.value}/2000 verified.")


if __name__ == "__main__":
    main()