import datetime

import dl_httpx


def test_no_timezone_sets_to_utc() -> None:
    dt = datetime.datetime(2025, 1, 2, 3, 4, 5)
    serialized_datetime = dl_httpx.serialize_datetime(dt)
    assert serialized_datetime == "2025-01-02T03:04:05.000000Z"


def test_with_microseconds() -> None:
    dt = datetime.datetime(2025, 1, 2, 3, 4, 5, 6)
    serialized_datetime = dl_httpx.serialize_datetime(dt)
    assert serialized_datetime == "2025-01-02T03:04:05.000006Z"


def test_with_timezone_utc() -> None:
    dt = datetime.datetime(2025, 1, 2, 3, 4, 5, 6, datetime.timezone.utc)
    serialized_datetime = dl_httpx.serialize_datetime(dt)
    assert serialized_datetime == "2025-01-02T03:04:05.000006Z"


def test_with_timezone_offset_3() -> None:
    dt = datetime.datetime(2025, 1, 2, 3, 4, 5, 6, datetime.timezone(datetime.timedelta(hours=3)))
    serialized_datetime = dl_httpx.serialize_datetime(dt)
    assert serialized_datetime == "2025-01-02T00:04:05.000006Z"
