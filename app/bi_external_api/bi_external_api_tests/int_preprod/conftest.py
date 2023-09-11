import os
from datetime import datetime
from typing import Any, Type

import aiohttp
import pytest
import pytz
import yaml

from bi_api_commons.base_models import TenantCommon
from bi_api_commons_ya_team.models import YaTeamAuthData
from bi_external_api.converter.workbook_ctx_loader import WorkbookContextLoader
from bi_external_api.domain import external as ext
from bi_external_api.domain.internal import (
    datasets,
)
from bi_defaults.environments import InternalTestingInstallation
from bi_external_api.domain.internal.dl_common import EntrySummary
from bi_external_api.enums import ExtAPIType
from bi_external_api.internal_api_clients.charts_api import APIClientCharts
from bi_api_commons.client.common import CommonInternalAPIClient
from bi_external_api.internal_api_clients.dash_api import APIClientDashboard
from bi_external_api.internal_api_clients.dataset_api import APIClientBIBackControlPlane
from bi_external_api.internal_api_clients.main import InternalAPIClients
from bi_external_api.internal_api_clients.united_storage import MiniUSClient
from bi_external_api.testings import PGSubSelectDatasetFactory, SingleTabDashboardBuilder
from bi_external_api.workbook_ops.facade import WorkbookOpsFacade

from bi_testing.env_params.generic import GenericEnvParamGetter

from ..test_acceptance import ConnectionTestingData


@pytest.fixture(scope='session')
def env_param_getter() -> GenericEnvParamGetter:
    filepath = os.path.join(os.path.dirname(__file__), 'params.yml')
    return GenericEnvParamGetter.from_yaml_file(filepath)


@pytest.fixture()
def pseudo_wb_path_value() -> str:
    now = datetime.now(tz=pytz.timezone("Europe/Moscow"))
    return f"ext_api_tests/{now.strftime('%y-%m-%d-%H:%M:%S.%f')}"


@pytest.fixture()
async def pseudo_wb_path(pseudo_wb_path_value, bi_ext_api_int_preprod_int_api_clients) -> str:
    now = datetime.now(tz=pytz.timezone("Europe/Moscow"))
    wb_id = f"ext_api_tests/{now.strftime('%y-%m-%d-%H:%M:%S.%f')}"
    await bi_ext_api_int_preprod_int_api_clients.us.create_folder(wb_id)
    return wb_id


@pytest.fixture(scope="function")
def bi_ext_api_int_preprod_int_api_clients(
        loop,
        intranet_user_1_creds,
) -> InternalAPIClients:
    session = aiohttp.ClientSession()

    def cli(clz: Type[CommonInternalAPIClient], url: str) -> CommonInternalAPIClient:
        return clz(
            session=session,
            base_url=url,
            tenant=TenantCommon(),
            auth_data=YaTeamAuthData(oauth_token=intranet_user_1_creds.token),
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


@pytest.fixture(scope="function")
def bi_ext_api_int_preprod_dash_api_client(bi_ext_api_int_preprod_int_api_clients) -> APIClientDashboard:
    return bi_ext_api_int_preprod_int_api_clients.dash


@pytest.fixture(scope="function")
def bi_ext_api_int_preprod_charts_api_client(bi_ext_api_int_preprod_int_api_clients) -> APIClientCharts:
    return bi_ext_api_int_preprod_int_api_clients.charts


@pytest.fixture(scope="function")
def bi_ext_api_int_preprod_bi_api_control_plane_client(
        bi_ext_api_int_preprod_int_api_clients
) -> APIClientBIBackControlPlane:
    return bi_ext_api_int_preprod_int_api_clients.datasets_cp


@pytest.fixture()
def wb_ctx_loader(bi_ext_api_int_preprod_int_api_clients) -> WorkbookContextLoader:
    return WorkbookContextLoader(
        internal_api_clients=bi_ext_api_int_preprod_int_api_clients,
        use_workbooks_api=False,
    )


@pytest.fixture(scope="session")
def pg_conn_data(secret_datalens_test_data) -> dict[str, Any]:
    pg_data = yaml.safe_load(secret_datalens_test_data["INTEGRATION_TESTS_INTERNAL_PG_1"])
    return pg_data


@pytest.fixture(scope="function")
async def pg_connection(
        pseudo_wb_path,
        bi_ext_api_int_preprod_bi_api_control_plane_client,
        pg_conn_data,
) -> datasets.ConnectionInstance:
    int_api_cli = bi_ext_api_int_preprod_bi_api_control_plane_client

    conn = await int_api_cli.create_connection(wb_id=pseudo_wb_path, name="pg_conn_main", conn_data=dict(
        type="postgres",
        host=pg_conn_data["host"],
        port=pg_conn_data["port"],
        username=pg_conn_data["username"],
        password=pg_conn_data["password"],
        db_name=pg_conn_data["database"],
        raw_sql_level="subselect",
        cache_ttl_sec=None,
    ))

    yield conn


@pytest.fixture(scope="session")
def chyt_connection_ext_value_and_secret(
        env_param_getter,
) -> tuple[ext.CHYTConnection, ext.Secret]:
    return ext.CHYTConnection(
        raw_sql_level=ext.RawSQLLevel.subselect,
        cache_ttl_sec=None,
        cluster="hahn",
        clique_alias="*ch_datalens",
    ), ext.PlainSecret(env_param_getter.get_str_value("YT_OAUTH"))


@pytest.fixture(scope="session")
def chyt_connection_testing_data_int_preprod(chyt_connection_ext_value_and_secret) -> ConnectionTestingData:
    chyt_conn, secret = chyt_connection_ext_value_and_secret

    return ConnectionTestingData(
        connection=chyt_conn,
        secret=secret,
        target_db_name=None,
        sample_super_store_schema_name=None,
        sample_super_store_table_name="//home/yandexbi/samples/sample_superstore",
        sample_super_store_sub_select_sql="SELECT * FROM `//home/yandexbi/samples/sample_superstore`",
        perform_tables_validation=False,
        perform_tables_fix=False,
    )


@pytest.fixture(scope="session")
def ch_connection_testing_data_int_preprod(secret_datalens_test_data) -> ConnectionTestingData:
    ch_data = yaml.safe_load(secret_datalens_test_data["INTEGRATION_TESTS_INTERNAL_CH_1"])
    ch_data["connection_type"] = "clickhouse"

    return ConnectionTestingData(
        connection=ext.ClickHouseConnection(
            host=ch_data["host"],
            port=ch_data["port"],
            username=ch_data["username"],
            secure="on",
            raw_sql_level=ext.RawSQLLevel.subselect,
            cache_ttl_sec=None,
        ),
        secret=ext.PlainSecret(ch_data["password"]),
        target_db_name=ch_data["database"],
        sample_super_store_schema_name=None,
        sample_super_store_table_name="ext_api_tests_int_preprod_sample_super_store",
        perform_tables_validation=True,
        perform_tables_fix=True,
    )


@pytest.fixture(scope="function")
def dataset_factory(
        pseudo_wb_path,
        bi_ext_api_int_preprod_bi_api_control_plane_client,
        pg_connection,
) -> PGSubSelectDatasetFactory:
    return PGSubSelectDatasetFactory(
        wb_id=pseudo_wb_path,
        bi_api_cli=bi_ext_api_int_preprod_bi_api_control_plane_client,
        conn_id=pg_connection.id,
    )


@pytest.fixture(scope="function")
def wb_ops_facade(bi_ext_api_int_preprod_int_api_clients, wb_ctx_loader) -> WorkbookOpsFacade:
    return WorkbookOpsFacade(
        internal_api_clients=bi_ext_api_int_preprod_int_api_clients,
        workbook_ctx_loader=wb_ctx_loader,
        api_type=ExtAPIType.YA_TEAM,
    )


@pytest.fixture(scope="function")
async def dashes_in_hierarchy(pseudo_wb_path, bi_ext_api_int_preprod_int_api_clients) -> set[EntrySummary]:
    dash_cli = bi_ext_api_int_preprod_int_api_clients.dash_strict
    us_cli = bi_ext_api_int_preprod_int_api_clients.us

    path_set = {
        "f1",
        "f1/ff1",
        "f1/ff2",
        "f1/ff2/fff1",
        "f1/ff2/fff2",
        "f2"
    }
    dash_summaries: set[EntrySummary] = set()

    for idx, sub_path in enumerate(sorted(path_set, key=lambda pp: tuple(pp.split("/")))):
        full_path = "/".join([pseudo_wb_path, sub_path])

        await us_cli.create_folder(full_path)
        dash_summary = await dash_cli.create_dashboard(
            SingleTabDashboardBuilder().build_dash(),
            workbook_id=full_path,
            name=f"target_dash_{idx}",
        )
        dash_summaries.add(dash_summary)

    return dash_summaries
