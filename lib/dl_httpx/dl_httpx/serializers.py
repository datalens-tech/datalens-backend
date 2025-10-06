import datetime


def serialize_datetime(value: datetime.datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)

    if value.tzinfo is not datetime.timezone.utc:
        value = value.astimezone(datetime.timezone.utc)

    return value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


__all__ = [
    "serialize_datetime",
]
