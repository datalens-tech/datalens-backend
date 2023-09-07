import json
import os
import random
from typing import Callable, Dict, Optional, ClassVar, Any

import attr
import boto3
import flask
import pytest
import requests
import shortuuid
from botocore.client import BaseClient

from bi_constants.api_constants import DLHeadersCommon

from bi_configs.crypto_keys import CryptoKeysConfig
from bi_configs.enums import AppType, EnvType

from bi_testing_ya.cloud_tokens import AccountCredentials
from bi_testing_ya.dlenv import DLEnv
from bi_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from bi_api_client.dsmaker.api.data_api import SyncHttpDataApiV1
from bi_testing.env_params.generic import GenericEnvParamGetter

from bi_core_testing.flask_utils import FlaskTestClient, FlaskTestResponse

import bi.app
from bi import app_async
from bi_api_lib.app_settings import ControlPlaneAppSettings, AsyncAppSettings, YCAuthSettings
from bi_api_lib.loader import load_bi_api_lib
from bi_api_lib.connector_availability.base import ConnectorAvailabilityConfig

from bi_api_lib_testing.client import TestClientConverterAiohttpToFlask, WrappedAioSyncApiClient, FlaskSyncApiClient

from bi_legacy_test_bundle_tests.api_lib.config import DB_PARAMS


class DataCloudTestingData:
    AWS_ACCOUNT_ID: ClassVar[str] = "177770737270"
    AWS_REGION: ClassVar[str] = "eu-central-1"

    US_BASE_URL: ClassVar[str] = "https://us.pp-preprod.bi.yadc.io"

    AWS_SECRET_ID_CRYPTO_KEYS: ClassVar[str] = (
        "arn:aws:secretsmanager:eu-central-1:177770737270:secret:dl-preprod-frk-pp-cry-k-cfg-CQImlk"
    )
    AWS_SECRET_ID_US_MASTER_TOKEN: ClassVar[str] = (
        "arn:aws:secretsmanager:eu-central-1:177770737270:secret:dl-preprod-frk-pp-us-master-token-2ceSkf"
    )


def _get_aws_secret_value(client: Any, secret_id: str) -> str:
    return client.get_secret_value(
        SecretId=secret_id,
        VersionStage='AWSCURRENT',
    )['SecretString']


@pytest.fixture(scope='session')
def env_param_getter() -> GenericEnvParamGetter:
    filepath = os.path.join(os.path.dirname(__file__), 'params.yml')
    return GenericEnvParamGetter.from_yaml_file(filepath)


@pytest.fixture(scope="session")
def dc_rs_secret_mgr_client(env_param_getter: GenericEnvParamGetter) -> BaseClient:
    session = boto3.session.Session()
    return session.client(
        service_name="secretsmanager",
        aws_access_key_id=env_param_getter.get_str_value("ACCESS_KEY_ID"),
        aws_secret_access_key=env_param_getter.get_str_value("SECRET_ACCESS_KEY"),
        region_name=DataCloudTestingData.AWS_REGION,
    )


@pytest.fixture(scope="session")
def dc_rs_crypto_keys_config(dc_rs_secret_mgr_client) -> CryptoKeysConfig:
    secret_plain_text = _get_aws_secret_value(
        dc_rs_secret_mgr_client,
        secret_id=DataCloudTestingData.AWS_SECRET_ID_CRYPTO_KEYS
    )
    return CryptoKeysConfig.from_json(json.loads(secret_plain_text))


@pytest.fixture(scope="session")
def dc_rs_us_master_token(dc_rs_secret_mgr_client) -> str:
    return _get_aws_secret_value(
        dc_rs_secret_mgr_client,
        secret_id=DataCloudTestingData.AWS_SECRET_ID_US_MASTER_TOKEN,
    )


@pytest.fixture(scope="session")
def dc_rc_user_account(integration_tests_admin_sa, dl_env) -> AccountCredentials:
    assert dl_env == DLEnv.dc_testing
    assert integration_tests_admin_sa is not None
    return integration_tests_admin_sa


@pytest.fixture(scope="session")
def dc_rs_us_base_url() -> str:
    return DataCloudTestingData.US_BASE_URL


#
# Control plane settings & apps
#

@pytest.fixture(scope="session")
def dc_rs_cp_app_settings(
        dl_env,
        rqe_config_subprocess,
        dc_rs_us_master_token,
        dc_rs_crypto_keys_config,
        dc_rs_us_base_url,
        ext_sys_requisites,
) -> ControlPlaneAppSettings:
    assert dl_env == DLEnv.dc_testing

    return ControlPlaneAppSettings(
        CONNECTOR_AVAILABILITY=ConnectorAvailabilityConfig(),
        APP_TYPE=AppType.DATA_CLOUD,
        ENV_TYPE=EnvType.dc_any,
        US_BASE_URL=dc_rs_us_base_url,
        US_MASTER_TOKEN=dc_rs_us_master_token,
        YC_AUTH_SETTINGS=YCAuthSettings(
            YC_AS_ENDPOINT=ext_sys_requisites.YC_AS_ENDPOINT,
            YC_AUTHORIZE_PERMISSION=ext_sys_requisites.YC_AUTHORIZE_PERMISSION,
        ),
        RQE_CONFIG=rqe_config_subprocess,
        CRYPTO_KEYS_CONFIG=dc_rs_crypto_keys_config,
    )


@pytest.fixture(scope="function")
def dc_rs_cp_app(dc_rs_cp_app_settings) -> flask.Flask:
    load_bi_api_lib()
    return bi.app.create_app(
        app_settings=dc_rs_cp_app_settings,
        connectors_settings={},
        close_loop_after_request=False,
    )


@pytest.fixture(scope='function')
def dc_rs_bi_api_client_factory(
        dc_rs_cp_app,
        dc_rc_user_account,
        dc_rs_project_id,
) -> Callable[[], FlaskTestClient]:
    def factory():
        class LocalClient(FlaskTestClient):
            def get_us_home_directory(self) -> str:
                """To split US directories for different users to prevent permissions issues"""
                return "unit_tests"

            def get_default_headers(self) -> Dict[str, Optional[str]]:
                return {
                    **super().get_default_headers(),
                    DLHeadersCommon.IAM_TOKEN.value: dc_rc_user_account.token,
                    "x-dc-projectid": dc_rs_project_id,
                }

        dc_rs_cp_app.test_client_class = LocalClient
        dc_rs_cp_app.response_class = FlaskTestResponse
        return dc_rs_cp_app.test_client()

    return factory


@pytest.fixture(scope='function')
def dc_rs_bi_api_client(dc_rs_bi_api_client_factory) -> FlaskTestClient:
    return dc_rs_bi_api_client_factory()


#
# Data plane settings & apps
#

@pytest.fixture(scope="session")
def dc_rs_dp_app_settings(
        dl_env,
        ext_sys_requisites,
        rqe_config_subprocess,
        dc_rs_us_master_token,
        dc_rs_crypto_keys_config,
        dc_rs_us_base_url,
) -> AsyncAppSettings:
    assert dl_env == DLEnv.dc_testing
    return AsyncAppSettings(
        APP_TYPE=AppType.DATA_CLOUD,
        US_BASE_URL=dc_rs_us_base_url,
        US_MASTER_TOKEN=dc_rs_us_master_token,
        YC_AUTH_SETTINGS=YCAuthSettings(
            YC_AS_ENDPOINT=ext_sys_requisites.YC_AS_ENDPOINT,
            YC_AUTHORIZE_PERMISSION=ext_sys_requisites.YC_AUTHORIZE_PERMISSION,
        ),
        RQE_CONFIG=rqe_config_subprocess,
        CRYPTO_KEYS_CONFIG=dc_rs_crypto_keys_config,
        CACHES_ON=False,
        CACHES_REDIS=None,
        BI_COMPENG_PG_ON=False,
    )


@pytest.fixture(scope='function')
def dc_rs_dp_low_level_client(loop, aiohttp_client, dc_rs_dp_app_settings):
    load_bi_api_lib()
    app = app_async.create_app(setting=dc_rs_dp_app_settings, connectors_settings={})
    return loop.run_until_complete(aiohttp_client(app))


@pytest.fixture(scope='function')
def dc_rs_data_api_v1(loop, dc_rs_project_id, dc_rc_user_account, dc_rs_dp_low_level_client) -> SyncHttpDataApiV1:
    return SyncHttpDataApiV1(
        client=WrappedAioSyncApiClient(
            int_wrapped_client=TestClientConverterAiohttpToFlask(
                loop=loop,
                aio_client=dc_rs_dp_low_level_client,
                extra_headers={
                    DLHeadersCommon.IAM_TOKEN.value: dc_rc_user_account.token,
                    "x-dc-projectid": dc_rs_project_id,
                }
            )
        )
    )


@attr.s(auto_attribs=True)
class DatasetAPISet:
    control_api_v1: SyncHttpDatasetApiV1
    data_api_v1: SyncHttpDataApiV1


@pytest.fixture(scope='function')
def dc_rs_ds_api_set(dc_rs_data_api_v1, dc_rs_bi_api_client) -> DatasetAPISet:
    api_set = DatasetAPISet(
        control_api_v1=SyncHttpDatasetApiV1(
            client=FlaskSyncApiClient(int_wclient=dc_rs_bi_api_client),
        ),
        data_api_v1=dc_rs_data_api_v1,
    )
    yield api_set
    api_set.control_api_v1.cleanup_created_resources()


#
# Entities
#

@pytest.fixture(scope="session")
def dc_rs_conn_data_clickhouse() -> Dict[str, Any]:
    ch_user = "datalens"
    ch_netloc, ch_password = DB_PARAMS["clickhouse"]
    ch_host, ch_port = ch_netloc.split(":")
    ch_port = int(ch_port)

    return {
        'secure': 'off',
        'host': ch_host,
        'port': ch_port,
        'username': ch_user,
        'password': ch_password,
    }


@pytest.fixture(scope="session")
def dc_rs_conn_data_postgres() -> Dict[str, Any]:
    pg_user = "datalens"
    pg_netloc, pg_password = DB_PARAMS["clickhouse"]
    pg_host, pg_port = pg_netloc.split(":")
    pg_port = int(pg_port)
    pg_db = "datalens"

    return {
        'host': pg_host,
        'port': pg_port,
        'db_name': pg_db,
        'username': pg_user,
        'password': pg_password,
    }


@pytest.fixture(scope="function")
def dc_rs_connection_id_clickhouse(dc_rs_conn_data_clickhouse, dc_rs_bi_api_client, dc_rs_workbook_id) -> str:
    conn_name = 'test__{}'.format(random.randint(0, 10000000))
    conn_data = {
        'name': conn_name,
        'workbook_id': dc_rs_workbook_id,
        #
        'type': 'clickhouse',
        **dc_rs_conn_data_clickhouse,
    }

    resp = dc_rs_bi_api_client.post(
        "/api/v1/connections",
        data=json.dumps(conn_data),
        content_type='application/json',
    )
    assert resp.status_code == 200, resp.json
    return resp.json["id"]


@pytest.fixture(scope="function")
def dc_rs_connection_id_postgres(dc_rs_conn_data_postgres, dc_rs_bi_api_client, dc_rs_workbook_id) -> str:
    conn_name = 'test__{}'.format(random.randint(0, 10000000))
    conn_data = {
        'name': conn_name,
        'workbook_id': dc_rs_workbook_id,
        #
        'type': 'postgres',
        **dc_rs_conn_data_postgres,
    }

    resp = dc_rs_bi_api_client.post(
        "/api/v1/connections",
        data=json.dumps(conn_data),
        content_type='application/json',
    )
    assert resp.status_code == 200, resp.json
    return resp.json["id"]


@pytest.fixture(scope="session")
def dc_rs_project_id(env_param_getter, dl_env: DLEnv) -> str:
    it_data_secret_entry_key = {
        DLEnv.dc_testing: "INTEGRATION_TESTS_DC_TESTING_DATA",
        DLEnv.dc_prod: "INTEGRATION_TESTS_DC_PROD_DATA",
    }.get(dl_env)

    assert it_data_secret_entry_key is not None
    return env_param_getter.get_yaml_value(it_data_secret_entry_key)['could_id']  # FIXME: cloud_id?


@pytest.fixture(scope="session")
def dc_rs_workbook_id(dc_rs_project_id, dc_rs_us_base_url, dc_rc_user_account):
    url = f"{dc_rs_us_base_url.strip('/')}/v2/workbooks"
    headers = {
        "x-dc-projectid": dc_rs_project_id,
        DLHeadersCommon.IAM_TOKEN.value: dc_rc_user_account.token,
    }

    wb_create_resp = requests.post(
        url=url,
        headers=headers,
        json=dict(
            title=f"Autotests {shortuuid.uuid()}"
        )
    )
    assert wb_create_resp.status_code == 200, wb_create_resp.content

    workbook_id = wb_create_resp.json()["workbookId"]
    yield workbook_id
    # delete_resp = requests.delete(f"{url}/{workbook_id}", headers=headers)
    # delete_resp.raise_for_status()
