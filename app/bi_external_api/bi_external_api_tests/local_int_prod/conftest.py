"""
This is module for local debug of external API with YaTeam prod installation.
It should not be launched in any CI.
Assumed that this tests will work with personal oAuth token of debugging person.
`${app_id}` (AKA oAuth client ID) = `6a8508f4746e43c284e1cca9ea1dc67b`.
ID of YAV secret with this token should be provided via env.
By default no any tests should be launched. Tests to launch (and target IDs) should be explicitly specified via env/PyTest params.

Personal oAuth token should be created manually by developer. To perform it open in browser next link:
`https://oauth.yandex-team.ru/authorize?response_type=token&client_id=${app_id}`

Then store it in YAV. Key should be `${app_id}`.
Assumed that it will be secret created by developer and he/she will be the only person who has access to this secret.
Secret ID should be set to env var `BI_EXT_API_DEV_DATA_SECRET_ID`.
"""

import os
from typing import Type

import aiohttp
import attr
import pytest

from bi_configs.environments import InternalProductionInstallation
from bi_constants.api_constants import DLHeadersCommon
from bi_api_commons.base_models import TenantCommon, YaTeamAuthData
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


@attr.s
class DevAuthData:
    oauth_yt_stat: str = attr.ib(repr=False)


@pytest.fixture(scope='session')
def env_param_getter() -> GenericEnvParamGetter:
    filepath = os.path.join(os.path.dirname(__file__), 'params.yml')
    filepath = get_file_loader().resolve_path(filepath)
    return GenericEnvParamGetter.from_yaml_file(filepath)


@pytest.fixture(scope="function")
def dev_auth_data(env_param_getter: GenericEnvParamGetter) -> DevAuthData:
    return DevAuthData(
        env_param_getter.get_str_value('OAUTH_YT_STAT')
    )


@pytest.fixture(scope="function")
def bi_ext_api_int_prod_su_int_api_clients(loop, dev_auth_data) -> InternalAPIClients:
    session = aiohttp.ClientSession()

    def cli(clz: Type[CommonInternalAPIClient], url: str) -> CommonInternalAPIClient:
        return clz(
            session=session,
            base_url=url,
            tenant=TenantCommon(),
            auth_data=YaTeamAuthData(oauth_token=dev_auth_data.oauth_yt_stat),
            use_workbooks_api=False,
            extra_headers={DLHeadersCommon.ALLOW_SUPERUSER: "1", DLHeadersCommon.SUDO: "1"},
            # To prevent any modifications on prod stand
            read_only=True,
        )

    yield InternalAPIClients(
        datasets_cp=cli(
            APIClientBIBackControlPlane,
            InternalProductionInstallation.DATALENS_API_LB_MAIN_BASE_URL
        ),
        charts=cli(
            APIClientCharts,
            "https://charts.yandex-team.ru"
        ),
        dash=cli(
            APIClientDashboard,
            "https://api.dash.yandex.net",
        ),
        us=cli(
            MiniUSClient,
            InternalProductionInstallation.US_BASE_URL,
        ),
    )
    loop.run_until_complete(session.close())


@pytest.fixture()
def bi_ext_api_int_prod_su_wb_ctx_loader(bi_ext_api_int_prod_su_int_api_clients) -> WorkbookContextLoader:
    return WorkbookContextLoader(
        internal_api_clients=bi_ext_api_int_prod_su_int_api_clients,
        use_workbooks_api=False,
    )


@pytest.fixture(scope="function")
def bi_ext_api_int_prod_su_wb_ops_facade(
        bi_ext_api_int_prod_su_int_api_clients,
        bi_ext_api_int_prod_su_wb_ctx_loader,
) -> WorkbookOpsFacade:
    return WorkbookOpsFacade(
        internal_api_clients=bi_ext_api_int_prod_su_int_api_clients,
        workbook_ctx_loader=bi_ext_api_int_prod_su_wb_ctx_loader,
        do_add_exc_message=True,
        api_type=ExtAPIType.CORE,
    )
