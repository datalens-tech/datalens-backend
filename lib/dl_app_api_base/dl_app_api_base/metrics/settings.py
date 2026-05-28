import pathlib

import pydantic

import dl_app_api_base.metrics.http_request_duration_seconds as http_request_duration_seconds
import dl_app_api_base.metrics.http_requests_total as http_requests_total
import dl_settings


class MetricsSettings(dl_settings.BaseSettings):
    PATH: str = "/system/metrics"
    MULTIPROC_DIR: pathlib.Path | None = None
    HTTP_REQUESTS_TOTAL: http_requests_total.HttpRequestsTotalSettings = pydantic.Field(
        default_factory=http_requests_total.HttpRequestsTotalSettings,
    )
    HTTP_REQUEST_DURATION_SECONDS: http_request_duration_seconds.HttpRequestDurationSecondsSettings = pydantic.Field(
        default_factory=http_request_duration_seconds.HttpRequestDurationSecondsSettings,
    )
