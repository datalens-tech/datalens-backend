"""
Common YandexCloud-auth module.
"""
from typing import Optional

from bi_cloud_integration.yc_client_base import (
    DLYCRetryPolicy,
    DLYCServiceConfig,
)

AS_DEFAULT_TIMEOUT_SEC = 2
AS_DEFAULT_RETRIES_COUNT = 7
AS_DEFAULT_KEEPALIVE_TIME_MSEC = 10 * 1000  # ms
AS_DEFAULT_KEEPALIVE_TIMEOUT_MSEC = 1 * 1000  # ms
AS_DEFAULT_USERAGENT = "datalens"


def make_default_yc_auth_service_config(
    endpoint: Optional[str],
    call_timeout: Optional[float] = None,
    total_timeout: Optional[float] = None,
) -> DLYCServiceConfig:
    assert endpoint is not None
    return DLYCServiceConfig(
        endpoint=endpoint,
        tls=True,
        keepalive_time_msec=AS_DEFAULT_KEEPALIVE_TIME_MSEC,
        keepalive_timeout_msec=AS_DEFAULT_KEEPALIVE_TIMEOUT_MSEC,
        retry_policy=DLYCRetryPolicy(
            call_timeout=(call_timeout if call_timeout is not None else AS_DEFAULT_TIMEOUT_SEC),
            total_timeout=(
                total_timeout
                if total_timeout is not None
                else AS_DEFAULT_TIMEOUT_SEC * (AS_DEFAULT_RETRIES_COUNT + 0.5)
            ),
        ),
        user_agent=AS_DEFAULT_USERAGENT,
    )
