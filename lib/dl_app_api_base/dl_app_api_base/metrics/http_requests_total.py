from typing import Self

import dl_prometheus
import dl_settings


class HttpRequestsTotalSettings(dl_settings.BaseSettings):
    pass


class HttpRequestsTotal(dl_prometheus.Counter):
    @classmethod
    def from_settings(cls, settings: HttpRequestsTotalSettings) -> Self:
        return cls(
            name="http_requests_total",
            documentation="Total number of HTTP requests handled, partitioned by method, status code, and path",
            labelnames=(
                "method",
                "status_code",
                "path",
            ),
        )

    def record(self, *, method: str, status_code: int, path: str) -> None:
        self.inc(labels={"method": method, "status_code": str(status_code), "path": path})
