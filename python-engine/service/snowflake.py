import os
import threading
import time


EPOCH_MILLIS = 1_704_067_200_000
MAX_NODE_ID = (1 << 10) - 1


class SnowflakeGenerator:
    def __init__(self, node_id: int) -> None:
        if not 0 <= node_id <= MAX_NODE_ID:
            raise ValueError(f"SNOWFLAKE_NODE_ID must be between 0 and {MAX_NODE_ID}")
        self.node_id = node_id
        self.last_millis = -1
        self.sequence = 0
        self.lock = threading.Lock()

    def next_id(self) -> str:
        with self.lock:
            now = time.time_ns() // 1_000_000
            if now < self.last_millis:
                raise RuntimeError("system clock moved backwards")
            if now == self.last_millis:
                self.sequence = (self.sequence + 1) & 0xFFF
                if self.sequence == 0:
                    while now <= self.last_millis:
                        time.sleep(0.001)
                        now = time.time_ns() // 1_000_000
            else:
                self.sequence = 0
            if now < EPOCH_MILLIS:
                raise RuntimeError("system clock precedes snowflake epoch")
            self.last_millis = now
            value = ((now - EPOCH_MILLIS) << 22) | (self.node_id << 12) | self.sequence
            return str(value)


_generator = SnowflakeGenerator(int(os.environ.get("SNOWFLAKE_NODE_ID", "2")))


def next_id() -> str:
    return _generator.next_id()
