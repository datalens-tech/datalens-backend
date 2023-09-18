from datetime import datetime
import os
from typing import (
    ClassVar,
    Type,
)

from aiochclient.http_clients import aiohttp
import boto3
import pytest
import pytz
import yaml

from bi_api_commons_ya_cloud.models import (
    ExternalIAMAuthData,
    IAMAuthData,
    TenantDCProject,
)
from bi_api_lib_ya.app_settings import YCAuthSettings
from bi_external_api.app import create_app
from bi_external_api.enums import ExtAPIType
from bi_external_api.grpc_proxy import server as grpc_proxy
import bi_external_api.grpc_proxy.ext_api_client
from bi_external_api.internal_api_clients.charts_api import APIClientCharts
from bi_external_api.internal_api_clients.dash_api import APIClientDashboard
from bi_external_api.internal_api_clients.dataset_api import APIClientBIBackControlPlane
from bi_external_api.internal_api_clients.main import InternalAPIClients
from bi_external_api.internal_api_clients.united_storage import MiniUSClient
from bi_external_api.settings import ExternalAPISettings
from bi_external_api.testings import WorkbookOpsClient
from bi_testing_ya.cloud_tokens import AccountCredentials
from bi_testing_ya.dlenv import DLEnv
from dl_api_commons.base_models import (
    NoAuthData,
    TenantCommon,
)
from dl_api_commons.client.common import CommonInternalAPIClient
from dl_configs.enums import AppType
from dl_testing.env_params.generic import GenericEnvParamGetter

from ..test_acceptance import ConnectionTestingData


class DoubleCloudTestingData:
    AWS_ACCOUNT_ID: ClassVar[str] = "177770737270"
    AWS_REGION: ClassVar[str] = "eu-central-1"

    ROBOT_YAV_SECRET_ID: ClassVar[str] = "sec-01f9pkp704tp2xw7cjs5g8qjgr"

    US_BASE_URL: ClassVar[str] = "https://us.pp-preprod.bi.yadc.io"
    BI_BACK_BASE_URL: ClassVar[str] = "https://back.pp-preprod.bi.yadc.io"
    DL_FRONT_BASE_URL: ClassVar[str] = "https://ui.pp-preprod.bi.yadc.io"
    DL_EXT_API_BASE_URL: ClassVar[str] = "https://api.bi.yadc.io"

    AWS_SECRET_ID_US_MASTER_TOKEN: ClassVar[
        str
    ] = "arn:aws:secretsmanager:eu-central-1:177770737270:secret:dl-preprod-frk-pp-us-master-token-2ceSkf"


# Override fixture from `lib/dl_testing`
@pytest.fixture(scope="session")
def dl_env() -> DLEnv:
    return DLEnv.dc_testing


@pytest.fixture(scope="session")
def env_param_getter() -> GenericEnvParamGetter:
    filepath = os.path.join(os.path.dirname(__file__), "params.yml")
    return GenericEnvParamGetter.from_yaml_file(filepath)


@pytest.fixture(scope="session")
def dc_rs_us_master_token(env_param_getter: GenericEnvParamGetter) -> str:
    session = boto3.session.Session()
    return session.client(
        service_name="secretsmanager",
        aws_access_key_id=env_param_getter.get_str_value("ACCESS_KEY_ID"),
        aws_secret_access_key=env_param_getter.get_str_value("SECRET_ACCESS_KEY"),
        region_name=DoubleCloudTestingData.AWS_REGION,
    ).get_secret_value(
        SecretId=DoubleCloudTestingData.AWS_SECRET_ID_US_MASTER_TOKEN,
        VersionStage="AWSCURRENT",
    )[
        "SecretString"
    ]


@pytest.fixture(scope="session")
def dc_rs_project_id(integration_tests_dc_project_id) -> str:
    return integration_tests_dc_project_id


@pytest.fixture(scope="session")
def dc_rc_user_account(integration_tests_admin_sa, dl_env) -> AccountCredentials:
    assert dl_env == DLEnv.dc_testing
    assert integration_tests_admin_sa is not None
    return integration_tests_admin_sa


@pytest.fixture(scope="function")
def bi_ext_api_dc_preprod_int_api_clients(
    loop,
    integration_tests_admin_sa,
    dc_rs_project_id,
) -> InternalAPIClients:
    session = aiohttp.ClientSession()

    def cli(clz: Type[CommonInternalAPIClient], url: str) -> CommonInternalAPIClient:
        return clz(
            session=session,
            base_url=url,
            tenant=TenantDCProject(project_id=dc_rs_project_id),
            auth_data=IAMAuthData(iam_token=integration_tests_admin_sa.token),
            use_workbooks_api=True,
        )

    yield InternalAPIClients(
        datasets_cp=cli(
            APIClientBIBackControlPlane,
            DoubleCloudTestingData.BI_BACK_BASE_URL,
        ),
        charts=cli(
            APIClientCharts,
            DoubleCloudTestingData.DL_FRONT_BASE_URL,
        ),
        dash=cli(
            APIClientDashboard,
            DoubleCloudTestingData.DL_FRONT_BASE_URL,
        ),
        us=cli(
            MiniUSClient,
            DoubleCloudTestingData.US_BASE_URL,
        ),
    )
    loop.run_until_complete(session.close())


@pytest.fixture()
def workbook_name() -> str:
    now = datetime.now(tz=pytz.timezone("Europe/Moscow"))
    return f"Autotests-{now.strftime('%y-%m-%d-%H:%M:%S.%f')}"


@pytest.fixture(scope="function")
async def workbook_id(bi_ext_api_dc_preprod_int_api_clients, workbook_name) -> str:
    cli = bi_ext_api_dc_preprod_int_api_clients.us

    wb_id = await cli.create_workbook(workbook_name)
    yield wb_id
    await cli.delete_workbook(wb_id)


@pytest.fixture(scope="function")
def bi_ext_api_dc_preprod_app(
    loop,
    aiohttp_client,
    ext_sys_requisites,
    dc_rs_us_master_token,
):
    settings = ExternalAPISettings(
        APP_TYPE=AppType.DATA_CLOUD,
        API_TYPE=ExtAPIType.DC,
        YC_AUTH_SETTINGS=YCAuthSettings(
            YC_AS_ENDPOINT=ext_sys_requisites.YC_AS_ENDPOINT,
            YC_AUTHORIZE_PERMISSION=ext_sys_requisites.YC_AUTHORIZE_PERMISSION,
        ),
        DATASET_CONTROL_PLANE_API_BASE_URL=DoubleCloudTestingData.BI_BACK_BASE_URL,
        DASH_API_BASE_URL=DoubleCloudTestingData.DL_FRONT_BASE_URL,
        CHARTS_API_BASE_URL=DoubleCloudTestingData.DL_FRONT_BASE_URL,
        US_BASE_URL=DoubleCloudTestingData.US_BASE_URL,
        US_MASTER_TOKEN=dc_rs_us_master_token,
    )

    app = create_app(settings)
    return loop.run_until_complete(aiohttp_client(app))


@pytest.fixture(scope="function")
def bi_ext_api_dc_preprod_app_base_url(bi_ext_api_dc_preprod_app) -> str:
    srv = bi_ext_api_dc_preprod_app.server
    return f"http://{srv.host}:{srv.port}"


@pytest.fixture(scope="function")
def bi_ext_api_dc_preprod_http_client_no_creds(
    bi_ext_api_dc_preprod_app_base_url,
    loop,
    dc_rs_project_id,
) -> WorkbookOpsClient:
    session = aiohttp.ClientSession()
    yield WorkbookOpsClient(
        base_url=bi_ext_api_dc_preprod_app_base_url,
        auth_data=NoAuthData(),
        tenant=TenantCommon(),
        api_type=ExtAPIType.DC,
    )
    loop.run_until_complete(session.close())


@pytest.fixture(scope="function")
def bi_ext_api_dc_preprod_http_client(
    bi_ext_api_dc_preprod_app_base_url,
    loop,
    integration_tests_admin_sa,
    dc_rs_project_id,
) -> WorkbookOpsClient:
    session = aiohttp.ClientSession()
    yield WorkbookOpsClient(
        base_url=bi_ext_api_dc_preprod_app_base_url,
        auth_data=ExternalIAMAuthData(iam_token=integration_tests_admin_sa.token),
        tenant=TenantCommon(),
        api_type=ExtAPIType.DC,
    )
    loop.run_until_complete(session.close())


@pytest.fixture(scope="function")
def bi_ext_api_dc_preprod_http_client_true(
    loop,
    integration_tests_admin_sa,
    dc_rs_project_id,
) -> WorkbookOpsClient:
    session = aiohttp.ClientSession()
    yield WorkbookOpsClient(
        base_url=DoubleCloudTestingData.DL_EXT_API_BASE_URL,
        auth_data=ExternalIAMAuthData(iam_token=integration_tests_admin_sa.token),
        tenant=TenantCommon(),
        api_type=ExtAPIType.DC,
    )
    loop.run_until_complete(session.close())


@pytest.fixture(scope="function")
def bi_ext_api_grpc_against_dc_preprod(
    dc_rc_user_account,
    bi_ext_api_dc_preprod_app_base_url,
):
    ext_api_endpoint = bi_ext_api_dc_preprod_app_base_url
    ext_api_client = bi_external_api.grpc_proxy.ext_api_client.ExtApiClient(
        base_url=ext_api_endpoint,
        ext_api_type=ExtAPIType.DC,
    )
    server = grpc_proxy.GrpcProxyProvider(max_workers=1).create_threadpool_grpc_server(ext_api_client=ext_api_client)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()

    class GrpcProxyMockCtx:
        grpc_endpoint = f"127.0.0.1:{port}"
        user_account = dc_rc_user_account

    yield GrpcProxyMockCtx

    server.stop(grace=False)
    server.wait_for_termination()


@pytest.fixture(scope="session")
def bi_ext_api_dc_preprod_ch_connection_testing_data(secret_datalens_test_data):
    ch_data = yaml.safe_load(secret_datalens_test_data["INTEGRATION_TESTS_PREPROD_CH_1"])
    db_name = ch_data["database"]

    # todo: remove ext import, use dumb dicts
    from bi_external_api.domain import external as ext

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
        target_db_name=db_name,
        sample_super_store_table_name="ext_api_tests_dc_preprod_sample_super_store",
        sample_super_store_schema_name=None,
        sample_super_store_sub_select_sql=f"SELECT * FROM `{db_name}`.`ext_api_tests_dc_preprod_sample_super_store`",
    )
