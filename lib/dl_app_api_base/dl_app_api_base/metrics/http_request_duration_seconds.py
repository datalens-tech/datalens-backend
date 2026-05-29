from collections.abc import Sequence
from typing import Self

import dl_prometheus
import dl_settings

HTTP_REQUEST_DURATION_SECONDS_DEFAULT_BUCKETS: tuple[float, ...] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
)


class HttpRequestDurationSecondsSettings(dl_settings.BaseSettings):
    BUCKETS: Sequence[float] = HTTP_REQUEST_DURATION_SECONDS_DEFAULT_BUCKETS


class HttpRequestDurationSeconds(dl_prometheus.Histogram):
    @classmethod
    def from_settings(cls, settings: HttpRequestDurationSecondsSettings) -> Self:
        return cls(
            name="http_request_duration_seconds",
            documentation="HTTP request duration in seconds, partitioned by method, status code, and path",
            labelnames=(
                "method",
                "status_code",
                "path",
            ),
            buckets=settings.BUCKETS,
        )
