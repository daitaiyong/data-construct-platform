"""Output adapter interfaces for future persistence integrations."""

from typing import Protocol


class Sink(Protocol):
    def write_batch(self, records: list[str]) -> None: ...


class MySQLSink:
    def write_batch(self, records: list[str]) -> None:
        del records
        raise NotImplementedError("MySQL sink is not configured")
