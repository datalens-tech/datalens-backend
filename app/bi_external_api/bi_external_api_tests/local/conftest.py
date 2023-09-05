import os
from typing import Type

import aiohttp
import pytest

from bi_configs.environments import InternalTestingInstallation
from bi_api_commons.base_models import TenantCommon
from bi_api_commons_ya_team.models import YaTeamAuthData
from bi_external_api.converter.workbook_ctx_loader import WorkbookContextLoader
from bi_external_api.enums import ExtAPIType
from bi_external_api.internal_api_clients.charts_api import APIClientCharts
from bi_api_commons.client.common import CommonInternalAPIClient
from bi_external_api.internal_api_clients.dash_api import APIClientDashboard
from bi_external_api.internal_api_clients.dataset_api import APIClientBIBackControlPlane
from bi_external_api.internal_api_clients.main import InternalAPIClients
from bi_external_api.internal_api_clients.united_storage import MiniUSClient
from bi_external_api.workbook_ops.facade import WorkbookOpsFacade

from bi_testing.env_params.generic import GenericEnvParamGetter
from bi_testing.files import get_file_loader

ENV_KEY_DO_NOT_SKIP = "BI_EXT_API_TEST_DO_NOT_SKIP"
ENV_KEY_YT_TOKEN = "BI_EXT_API_TESTS_MAIN_YT_TOKEN"
ENV_KET_API_TOKEN = "BI_EXT_API_TESTS_MAIN_STAT_TOKEN"


@pytest.fixture(scope='session')
def env_param_getter() -> GenericEnvParamGetter:
    filepath = os.path.join(os.path.dirname(__file__), 'params.yml')
    filepath = get_file_loader().resolve_path(filepath)
    return GenericEnvParamGetter.from_yaml_file(filepath)


@pytest.fixture(scope="function")
def bi_ext_api_int_preprod_int_api_clients(loop) -> InternalAPIClients:
    session = aiohttp.ClientSession()

    def cli(clz: Type[CommonInternalAPIClient], url: str) -> CommonInternalAPIClient:
        return clz(
            session=session,
            base_url=url,
            tenant=TenantCommon(),
            auth_data=YaTeamAuthData(oauth_token=os.environ[ENV_KET_API_TOKEN]),
            use_workbooks_api=False,
        )

    yield InternalAPIClients(
        datasets_cp=cli(
            APIClientBIBackControlPlane,
            InternalTestingInstallation.DATALENS_API_LB_MAIN_BASE_URL
        ),
        charts=cli(
            APIClientCharts,
            "https://charts-beta.yandex-team.ru"
        ),
        dash=cli(
            APIClientDashboard,
            "https://api-test.dash.yandex.net",
        ),
        us=cli(
            MiniUSClient,
            InternalTestingInstallation.US_BASE_URL,
        ),
    )
    loop.run_until_complete(session.close())


@pytest.fixture()
def wb_ctx_loader(bi_ext_api_int_preprod_int_api_clients) -> WorkbookContextLoader:
    return WorkbookContextLoader(
        internal_api_clients=bi_ext_api_int_preprod_int_api_clients,
        use_workbooks_api=False,
    )


@pytest.fixture(scope="function")
def wb_ops_facade(bi_ext_api_int_preprod_int_api_clients, wb_ctx_loader) -> WorkbookOpsFacade:
    return WorkbookOpsFacade(
        internal_api_clients=bi_ext_api_int_preprod_int_api_clients,
        workbook_ctx_loader=wb_ctx_loader,
        api_type=ExtAPIType.CORE,
    )
