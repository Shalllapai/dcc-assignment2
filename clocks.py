import threading


class LamportClock:
    def __init__(self, initial_time: int = 0):
        self._value = initial_time
        self._lock = threading.Lock()

    def tick(self) -> int:
        with self._lock:
            self._value += 1
            return self._value

    def update(self, received_time: int) -> int:
        with self._lock:
            self._value = max(self._value, received_time) + 1
            return self._value

    def get_time(self) -> int:
        with self._lock:
            return self._value