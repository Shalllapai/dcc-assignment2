# Replicated Counter Service

### 1. Setup & Environment

python -m venv .venv
# On Windows:
.venv\Scripts\Activate.ps1

pip install grpcio grpcio-tools pytest
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. counter.proto

### 2. Start Single Replica

python server.py --node-id replica-A --port 50051

### 3. Start Three Replicas (Cluster)

python cluster.py

### 4. Run Client Commands

# Increment counter
python client.py incr likes:post-42 --by 1

# Retry with same idempotency key
python client.py incr likes:post-42 --by 1 --key test-fixed-key

# Get counter value
python client.py get likes:post-42

### 5. Run Test Suite

# Unit & Integration tests (Task C2)
python -m pytest -v tests/test_counter.py

# Failure-injection tests (Task C3)
python -m pytest -v tests/test_failures.py

### 6. Run Benchmark (Task C4)

python perf_benchmark.py