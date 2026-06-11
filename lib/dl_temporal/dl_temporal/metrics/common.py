import enum


class TemporalExecutionStatus(enum.StrEnum):
    SUCCESS = "success"
    ERROR = "error"
    FAILURE = "failure"


DURATION_SECONDS_DEFAULT_BUCKETS: tuple[float, ...] = (
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
    600.0,
    1800.0,
    3600.0,
)


__all__ = [
    "DURATION_SECONDS_DEFAULT_BUCKETS",
    "TemporalExecutionStatus",
]
