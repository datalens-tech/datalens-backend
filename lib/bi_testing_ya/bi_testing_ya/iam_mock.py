from __future__ import annotations

from typing import (
    Any,
    Generator,
)

from bi_cloud_integration.iam_mock import (
    FAKE_SERVICE_LOCAL_METADATA_IAM_TOKEN,
    AuthConfigHolder,
    IAMServicesMockFacade,
)
import bi_cloud_integration.local_metadata
from bi_cloud_integration.yc_client_base import DLYCServiceConfig


async def get_yc_service_token_local_details_mock(*args: Any, **kwargs: Any) -> tuple[str, int]:
    return FAKE_SERVICE_LOCAL_METADATA_IAM_TOKEN, 1


def apply_iam_services_mock(monkeypatch: Any) -> Generator[IAMServicesMockFacade, None, None]:
    auth_config_holder = AuthConfigHolder()

    monkeypatch.setattr(
        bi_cloud_integration.local_metadata,
        "get_yc_service_token_local_details",
        get_yc_service_token_local_details_mock,
    )

    server = IAMServicesMockFacade.create_threadpool_grpc_server(auth_config_holder)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    yield IAMServicesMockFacade(
        data_holder=auth_config_holder,
        service_config=DLYCServiceConfig(
            endpoint=f"grpc://127.0.0.1:{port}",
        ),
    )
    server.stop(grace=False)
    server.wait_for_termination()
