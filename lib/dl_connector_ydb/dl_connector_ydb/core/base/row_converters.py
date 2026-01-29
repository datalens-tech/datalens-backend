import datetime
from typing import (
    Any,
    Callable,
)


def convert_list_of_string(value: list[bytes | None]) -> list[str | None]:
    return [v.decode("utf-8", errors="replace") if v is not None else None for v in value]


def convert_bytes(value: bytes) -> str:
    return value.decode("utf-8", errors="replace")


def convert_interval(value: datetime.timedelta | int) -> int:
    if isinstance(value, datetime.timedelta):
        return int(value.total_seconds() * 1_000_000)
    return value


def convert_timestamp(value: int | datetime.datetime) -> datetime.datetime:
    if isinstance(value, datetime.datetime):
        return value.replace(tzinfo=datetime.timezone.utc)
    return datetime.datetime.utcfromtimestamp(value / 1e6).replace(tzinfo=datetime.timezone.utc)


ROW_CONVERTERS: dict[str, Callable[[Any], Any]] = {
    "list<string>": convert_bytes,
    "string": convert_bytes,
    "timestamp": convert_timestamp,
    "timestamp64": convert_timestamp,
    "interval": convert_interval,
    "interval64": convert_interval,
}
