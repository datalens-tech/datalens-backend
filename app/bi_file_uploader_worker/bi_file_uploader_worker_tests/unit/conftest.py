import pytest

from bi_api_commons.base_models import TenantCommon, NoAuthData
from bi_api_commons.client.base import DLCommonAPIClient
from bi_file_uploader_worker.app_health_check_lib import FileUploaderWorkerHealthCheckAppFactory


@pytest.fixture(scope="function")
def health_check_app(loop, aiohttp_client):
    app_factory = FileUploaderWorkerHealthCheckAppFactory()
    app = app_factory.create_app()
    return loop.run_until_complete(aiohttp_client(app))


@pytest.fixture(scope="function")
def health_check_api_client(health_check_app) -> DLCommonAPIClient:
    return DLCommonAPIClient(
        base_url=f"http://{health_check_app.host}:{health_check_app.port}",
        tenant=TenantCommon(),
        auth_data=NoAuthData(),
    )
