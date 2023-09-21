from __future__ import annotations

import asyncio
from threading import Thread
from typing import (
    Any,
    Tuple,
    Type,
)
from uuid import uuid4

import aiohttp.pytest_plugin
import aiohttp.test_utils
import bi.app
import pytest
from werkzeug.serving import make_server

from bi_api_commons_ya_cloud.models import IAMAuthData
from bi_api_lib_testing_ya.configuration import CONNECTOR_WHITELIST
from bi_api_lib_ya.app_settings import (
    ControlPlaneAppSettings,
    YCAuthSettings,
)
from bi_defaults.environments import InternalTestingInstallation
from bi_defaults.yenv_type import AppType
from bi_external_api.app import create_app
from bi_external_api.domain import external as ext
from bi_external_api.enums import ExtAPIType
from bi_external_api.internal_api_clients.dataset_api import APIClientBIBackControlPlane
from bi_external_api.internal_api_clients.main import InternalAPIClients
from bi_external_api.internal_api_clients.united_storage import MiniUSClient
from bi_external_api.settings import ExternalAPISettings
from bi_external_api.testings import WorkbookOpsClient
from bi_testing_ya.api_wrappers import APIClient
from bi_testing_ya.iam_mock import apply_iam_services_mock
from dl_api_commons.base_models import (
    NoAuthData,
    TenantCommon,
)
from dl_api_lib.app_settings import ControlApiAppTestingsSettings
from dl_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from dl_api_lib.loader import (
    ApiLibraryConfig,
    load_api_lib,
    preload_api_lib,
)
from dl_configs.rqe import (
    RQEBaseURL,
    RQEConfig,
)
from dl_constants.enums import USAuthMode
from dl_core.loader import CoreLibraryConfig
from dl_core_testing.environment import (
    common_pytest_configure,
    prepare_united_storage,
)
from dl_core_testing.fixture_server_runner import WSGIRunner

from .config import (
    DockerComposeEnvBiExtApi,
    DockerComposeEnvBiExtApiDC,
    TestingUSConfig,
)


pytest_plugins = (
    "aiohttp.pytest_plugin",
    "bi_testing_ya.pytest_plugin",
)

try:
    del aiohttp.pytest_plugin.loop
except AttributeError:
    pass


def pytest_configure(config: Any) -> None:  # noqa
    common_pytest_configure(tracing_service_name="tests_bi_ext_api")


@pytest.fixture(autouse=True)
def loop(event_loop):
    """
    Preventing creation of new loop by `aiohttp.pytest_plugin` loop fixture in favor of pytest-asyncio one
    And set loop pytest-asyncio created loop as default for thread
    """
    asyncio.set_event_loop(event_loop)
    return event_loop


@pytest.fixture(scope="session")
def bi_ext_api_test_env() -> DockerComposeEnvBiExtApi:
    """This fixture must be used only in db/ext tests"""
    return DockerComposeEnvBiExtApi()


@pytest.fixture(scope="session")
def bi_ext_api_dc_test_env() -> DockerComposeEnvBiExtApi:
    """This fixture must be used only in db/ext tests"""
    return DockerComposeEnvBiExtApiDC()


@pytest.fixture(scope="session")
def sync_rqe_netloc_subprocess(bi_ext_api_test_env) -> RQEBaseURL:
    with WSGIRunner(
        module="dl_core.bin.query_executor_sync",
        callable="app",
        ping_path="/ping",
        env=dict(
            EXT_QUERY_EXECUTER_SECRET_KEY=bi_ext_api_test_env.EXT_QUERY_EXECUTOR_SECRET_KEY,
            DEV_LOGGING="1",
        ),
    ) as runner:
        yield RQEBaseURL(host=runner.bind_addr, port=runner.bind_port)


# TODO FIX: Implement async RQE in subprocess
@pytest.fixture(scope="session")
def async_rqe_netloc_subprocess() -> RQEBaseURL:
    return RQEBaseURL(
        host="127.0.0.1",
        port=65500,
    )


@pytest.fixture(scope="session")
def rqe_config_subprocess(
    sync_rqe_netloc_subprocess,
    async_rqe_netloc_subprocess,
    bi_ext_api_test_env,
) -> RQEConfig:
    return RQEConfig(
        hmac_key=bi_ext_api_test_env.EXT_QUERY_EXECUTOR_SECRET_KEY.encode(),
        ext_sync_rqe=sync_rqe_netloc_subprocess,
        ext_async_rqe=async_rqe_netloc_subprocess,
        int_sync_rqe=sync_rqe_netloc_subprocess,
        int_async_rqe=async_rqe_netloc_subprocess,
    )


@pytest.fixture(scope="session")
def bi_ext_api_test_env_us_config(bi_ext_api_test_env) -> TestingUSConfig:
    us_config = bi_ext_api_test_env.us_config

    prepare_united_storage(
        us_host=us_config.base_url,
        us_master_token=us_config.master_token,
        us_pg_dsn=us_config.psycopg2_pg_dns,
        force=us_config.force_clear_db_on_launch,
    )

    return us_config


@pytest.fixture(scope="session")
def bi_ext_api_dc_test_env_us_config(bi_ext_api_dc_test_env) -> TestingUSConfig:
    us_config = bi_ext_api_dc_test_env.us_config

    # todo: comment during debug for faster runs
    prepare_united_storage(
        us_host=us_config.base_url,
        us_master_token=us_config.master_token,
        us_pg_dsn=us_config.psycopg2_pg_dns,
        force=us_config.force_clear_db_on_launch,
    )

    return us_config


def _make_control_plane_app(us_config, rqe_config_subprocess, iam_services_mock):
    preload_api_lib()
    settings = ControlPlaneAppSettings(
        CONNECTOR_AVAILABILITY=ConnectorAvailabilityConfig.from_settings(
            InternalTestingInstallation.CONNECTOR_AVAILABILITY
        ),
        APP_TYPE=AppType.TESTS,
        US_BASE_URL=us_config.base_url,
        US_MASTER_TOKEN=us_config.master_token,
        CRYPTO_KEYS_CONFIG=us_config.crypto_keys_config,
        DLS_HOST=InternalTestingInstallation.DATALENS_API_LB_DLS_BASE_URL,
        DLS_API_KEY="_tests_dls_api_key_",
        BI_COMPENG_PG_ON=False,
        BI_COMPENG_PG_URL=None,
        DO_DSRC_IDX_FETCH=False,
        RQE_CONFIG=rqe_config_subprocess,
        YC_AUTH_SETTINGS=YCAuthSettings(  # required for the RLS
            YC_AS_ENDPOINT=iam_services_mock.service_config.endpoint,
            YC_API_ENDPOINT_IAM=iam_services_mock.service_config.endpoint,
            YC_AUTHORIZE_PERMISSION=None,
        ),
        YC_RM_CP_ENDPOINT=iam_services_mock.service_config.endpoint,
        BI_API_CONNECTOR_WHITELIST=CONNECTOR_WHITELIST,
        CORE_CONNECTOR_WHITELIST=CONNECTOR_WHITELIST,
    )

    load_api_lib(
        ApiLibraryConfig(
            api_connector_ep_names=settings.BI_API_CONNECTOR_WHITELIST,
            core_lib_config=CoreLibraryConfig(core_connector_ep_names=settings.CORE_CONNECTOR_WHITELIST),
        )
    )
    app = bi.app.create_app(
        app_settings=settings,
        connectors_settings={},
        testing_app_settings=ControlApiAppTestingsSettings(
            us_auth_mode_override=USAuthMode.regular,
        ),
    )
    app.config["WE_ARE_IN_TESTS"] = True
    return app


def _make_cp_app_netloc(us_config, rqe_config_subprocess, iam_services_mock):
    bind_address = "localhost"
    cp_app = _make_control_plane_app(us_config, rqe_config_subprocess, iam_services_mock)
    server = make_server(
        app=cp_app,
        host=bind_address,
        port=0,
    )
    thread = Thread(target=lambda: server.serve_forever())
    thread.start()
    yield server.host, server.port
    server.shutdown()
    thread.join()


@pytest.fixture(scope="function")
def bi_ext_api_test_env_bi_api_control_plane_app_netloc(
    bi_ext_api_test_env_us_config, rqe_config_subprocess, iam_services_mock
) -> Tuple[str, int]:
    us_config = bi_ext_api_test_env_us_config

    yield from _make_cp_app_netloc(us_config, rqe_config_subprocess, iam_services_mock)


@pytest.fixture(scope="function")
def bi_ext_api_dc_test_env_bi_api_control_plane_app_netloc(
    bi_ext_api_dc_test_env_us_config, rqe_config_subprocess, iam_services_mock
) -> Tuple[str, int]:
    us_config = bi_ext_api_dc_test_env_us_config

    yield from _make_cp_app_netloc(us_config, rqe_config_subprocess, iam_services_mock)


def _make_int_api_clients(loop, netloc, us_config):
    session = aiohttp.ClientSession()

    def cli(clz: Type[APIClient], url: str) -> APIClient:
        return clz(
            session=session,
            base_url=url,
            tenant=TenantCommon(),
            auth_data=NoAuthData(),
        )

    yield InternalAPIClients(
        datasets_cp=cli(
            APIClientBIBackControlPlane,
            f"http://{netloc[0]}:{netloc[1]}/",
        ),
        charts=None,
        dash=None,
        us=cli(
            MiniUSClient,
            us_config.base_url,
        ),
    )
    loop.run_until_complete(session.close())


@pytest.fixture(scope="function")
def bi_ext_api_test_env_int_api_clients(
    loop,
    bi_ext_api_test_env_bi_api_control_plane_app_netloc,
    bi_ext_api_test_env_us_config,
) -> InternalAPIClients:
    netloc = bi_ext_api_test_env_bi_api_control_plane_app_netloc
    us_config = bi_ext_api_test_env_us_config
    yield from _make_int_api_clients(loop, netloc, us_config)


@pytest.fixture(scope="function")
def bi_ext_api_dc_test_env_int_api_clients(
    loop,
    bi_ext_api_dc_test_env_bi_api_control_plane_app_netloc,
    bi_ext_api_dc_test_env_us_config,
) -> InternalAPIClients:
    netloc = bi_ext_api_dc_test_env_bi_api_control_plane_app_netloc
    us_config = bi_ext_api_dc_test_env_us_config

    yield from _make_int_api_clients(loop, netloc, us_config)


@pytest.fixture(scope="function")
def bi_ext_api_test_env_bi_api_control_plane_client(
    loop,
    bi_ext_api_test_env_int_api_clients,
) -> APIClientBIBackControlPlane:
    return bi_ext_api_test_env_int_api_clients.datasets_cp


@pytest.fixture(scope="function")
def bi_ext_api_dc_test_env_bi_api_control_plane_client(
    loop,
    bi_ext_api_dc_test_env_int_api_clients,
) -> APIClientBIBackControlPlane:
    return bi_ext_api_dc_test_env_int_api_clients.datasets_cp


@pytest.fixture(scope="function")
def bi_ext_api_app(
    loop,
    aiohttp_client,
    bi_ext_api_test_env_bi_api_control_plane_app_netloc,
    bi_ext_api_test_env_us_config,
):
    bi_api_host, bi_api_port = bi_ext_api_test_env_bi_api_control_plane_app_netloc

    settings = ExternalAPISettings(
        APP_TYPE=AppType.TESTS,
        API_TYPE=ExtAPIType.CORE,
        YC_AUTH_SETTINGS=None,
        DATASET_CONTROL_PLANE_API_BASE_URL=f"http://{bi_api_host}:{bi_api_port}/",
        US_BASE_URL=bi_ext_api_test_env_us_config.base_url,
    )

    app = create_app(settings)
    return loop.run_until_complete(aiohttp_client(app))


@pytest.fixture(scope="function")
def bi_ext_api_client(bi_ext_api_app, loop) -> WorkbookOpsClient:
    app = bi_ext_api_app
    session = aiohttp.ClientSession()

    yield WorkbookOpsClient(
        base_url=f"http://{app.server.host}:{app.server.port}",
        auth_data=NoAuthData(),
        tenant=TenantCommon(),
        api_type=ExtAPIType.CORE,
    )

    loop.run_until_complete(session.close())


@pytest.fixture(params=[ExtAPIType.CORE, ExtAPIType.YA_TEAM, ExtAPIType.DC])
def bi_ext_api_types_all(request) -> ExtAPIType:
    yield request.param


@pytest.fixture(scope="session")
def bi_ext_api_iam_dc_endpoint() -> str:
    host = DockerComposeEnvBiExtApiDC.IAM_HOST
    port = DockerComposeEnvBiExtApiDC.IAM_PORT

    return f"{host}:{port}"


@pytest.fixture(scope="session")
def bi_ext_api_dc_auth(
    bi_ext_api_iam_dc_endpoint,
):
    return YCAuthSettings(
        YC_AS_ENDPOINT=bi_ext_api_iam_dc_endpoint,
        YC_AUTHORIZE_PERMISSION="datalens.objects.read",
    )


@pytest.fixture(scope="function")
def bi_ext_api_dc_app(
    loop,
    aiohttp_client,
    bi_ext_api_dc_test_env_bi_api_control_plane_app_netloc,
    bi_ext_api_dc_test_env_us_config,
    bi_ext_api_dc_auth,
):
    bi_api_host, bi_api_port = bi_ext_api_dc_test_env_bi_api_control_plane_app_netloc

    settings = ExternalAPISettings(
        APP_TYPE=AppType.TESTS,
        API_TYPE=ExtAPIType.DC,
        YC_AUTH_SETTINGS=bi_ext_api_dc_auth,
        DATASET_CONTROL_PLANE_API_BASE_URL=f"http://{bi_api_host}:{bi_api_port}/",
        US_BASE_URL=bi_ext_api_dc_test_env_us_config.base_url,
        US_MASTER_TOKEN=bi_ext_api_dc_test_env_us_config.master_token,
    )

    app = create_app(settings)
    return loop.run_until_complete(aiohttp_client(app))


@pytest.fixture(scope="function")
def bi_ext_api_dc_client(
    bi_ext_api_dc_app,
    loop,
) -> WorkbookOpsClient:
    app = bi_ext_api_dc_app
    session = aiohttp.ClientSession()

    yield WorkbookOpsClient(
        base_url=f"http://{app.server.host}:{app.server.port}",
        # auth_data=NoAuthData(),
        auth_data=IAMAuthData(iam_token="test-master-token"),
        tenant=TenantCommon(),
        api_type=ExtAPIType.DC,
    )

    loop.run_until_complete(session.close())


@pytest.fixture(scope="function")
async def bi_ext_api_dc_tmp_wb_id(
    bi_ext_api_dc_client,
) -> str:
    client = bi_ext_api_dc_client
    wb_title = uuid4().hex
    resp = await client.dc_create_workbook(
        ext.DCOpWorkbookCreateRequest(
            project_id="dummy-project-id",
            workbook_title=wb_title,
        )
    )
    wb_id = resp.workbook_id
    yield wb_id
    await client.dc_delete_workbook(
        ext.DCOpWorkbookDeleteRequest(
            workbook_id=wb_id,
        )
    )


@pytest.fixture()
def iam_services_mock(monkeypatch):
    yield from apply_iam_services_mock(monkeypatch)
