import subprocess
import sys
import time


def main():
    commands = [
        [sys.executable, "server.py", "--node-id", "replica-A", "--port", "50051"],
        [sys.executable, "server.py", "--node-id", "replica-B", "--port", "50052"],
        [sys.executable, "server.py", "--node-id", "replica-C", "--port", "50053"],
    ]

    processes = [subprocess.Popen(cmd) for cmd in commands]
    print("Cluster of 3 replicas started (ports 50051, 50052, 50053). Press Ctrl+C to terminate.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping cluster...")
        for p in processes:
            p.terminate()
        for p in processes:
            p.wait()
        print("Cluster stopped cleanly.")


if __name__ == "__main__":
    main()