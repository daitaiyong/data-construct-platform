from collections.abc import Mapping

from parser import render


MAX_RECORDS_PER_REQUEST = 10_000


def generate(template: str, variables: Mapping[str, str], count: int) -> list[str]:
    if count < 0 or count > MAX_RECORDS_PER_REQUEST:
        raise ValueError(f"record_count must be between 0 and {MAX_RECORDS_PER_REQUEST}")
    return [render(template, variables) for _ in range(count)]
