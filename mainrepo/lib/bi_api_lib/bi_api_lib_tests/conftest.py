from __future__ import annotations

import asyncio
import json
import os
import uuid
from base64 import b64encode
from typing import Dict, NamedTuple, Optional, Union

import aiobotocore.client
import aiohttp.pytest_plugin
import attr
import pytest

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric import rsa
from pytest_lazyfixture import lazy_fixture

from bi_connector_bundle_chs3.chs3_gsheets.core.constants import CONNECTION_TYPE_GSHEETS_V2
from bi_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE
from bi_connector_clickhouse.core.constants import CONNECTION_TYPE_CLICKHOUSE
from bi_connector_chyt.core.constants import CONNECTION_TYPE_CHYT
from bi_connector_yql.core.ydb.constants import CONNECTION_TYPE_YDB
from bi_connector_greenplum.core.constants import CONNECTION_TYPE_GREENPLUM
from bi_connector_metrica.core.constants import CONNECTION_TYPE_METRICA_API, CONNECTION_TYPE_APPMETRICA_API
from bi_connector_yql.core.yq.constants import CONNECTION_TYPE_YQ

from bi_constants.enums import ConnectionType, CreateDSFrom, DataSourceCreatedVia, RawSQLLevel

import bi_api_lib_tests.config as tests_config_mod
from bi_api_lib.loader import load_bi_api_lib
from bi_api_lib.app_settings import (
    AsyncAppSettings, ControlPlaneAppSettings, ControlPlaneAppTestingsSettings,
    YCAuthSettings, TestAppSettings, MDBSettings,
)
from bi_api_lib.connector_availability.base import ConnectorAvailabilityConfig
from bi_api_lib.service_registry.dataset_validator_factory import DefaultDatasetValidatorFactory
from bi_api_lib.service_registry.service_registry import BiApiServiceRegistry, DefaultBiApiServiceRegistry
from bi_configs import env_var_definitions
from bi_configs.connectors_settings import (
    FileS3ConnectorSettings,
    YQConnectorSettings,
    CHFrozenDemoConnectorSettings,
    MetricaConnectorSettings,
    AppmetricaConnectorSettings,
    YDBConnectorSettings,
    ClickHouseConnectorSettings,
    GreenplumConnectorSettings,
    MysqlConnectorSettings,
    PostgresConnectorSettings,
    CHYTConnectorSettings,
)
from bi_configs.crypto_keys import get_dummy_crypto_keys_config
from bi_configs.enums import AppType, EnvType
from bi_configs.rqe import RQEConfig
from bi_configs.settings_submodels import S3Settings, GoogleAppSettings

from bi_db_testing.loader import load_bi_db_testing

from bi_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from bi_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from bi_api_client.dsmaker.api.data_api import SyncHttpDataApiV1, SyncHttpDataApiV1_5, SyncHttpDataApiV2

from bi_api_commons.base_models import RequestContextInfo, TenantCommon
from bi_api_commons.base_models import TenantYCFolder
from bi_api_commons.reporting import DefaultReportingRegistry
from bi_core.components.ids import FieldIdGeneratorType
from bi_core.connections_security.base import InsecureConnectionSecurityManager
from bi_connector_bundle_ch_frozen.ch_frozen_base.core.us_connection import ConnectionClickhouseFrozenBase
from bi_core.logging_config import add_ylog_context
from bi_core.mdb_utils import MDBDomainManagerFactory
from bi_core.services_registry.conn_executor_factory import DefaultConnExecutorFactory
from bi_core_testing.connection import make_saved_connection
from bi_core_testing.database import (
    Db, CoreDbDispenser, CoreReInitableDbDispenser,
    make_table, make_table_with_arrays, make_db_config,
)
from bi_core_testing.environment import common_pytest_configure, restart_container_by_label
from bi_core.united_storage_client import USAuthContextMaster
from bi_core.us_manager.mutation_cache.usentry_mutation_cache_factory import DefaultUSEntryMutationCacheFactory
from bi_core.us_manager.us_manager import USManagerBase
from bi_core.us_manager.us_manager_async import AsyncUSManager
from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_core_testing.configuration import CoreTestEnvironmentConfigurationBase
from bi_core.utils import FutureRef
from bi_task_processor.processor import LocalTaskProcessorFactory, DummyTaskProcessorFactory
from bi_testing.s3_utils import create_s3_client, create_s3_bucket
from bi_testing.utils import wait_for_initdb
from bi_testing.containers import get_test_container_hostport
from statcommons.logs import LOGMUTATORS

from bi_api_lib.query.registry import register_for_connectors_with_native_wf

from bi_file_uploader_worker_lib.app import FileUploaderContextFab
from bi_file_uploader_worker_lib.settings import FileUploaderWorkerSettings
from bi_file_uploader_worker_lib.tasks import REGISTRY as FILE_UPLOADER_WORKER_TASK_REGISTRY

from bi_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration
from bi_api_lib_testing.app import RQEConfigurationMaker, RedisSettingMaker
from bi_api_lib_testing.client import TestClientConverterAiohttpToFlask, WrappedAioSyncApiClient, FlaskSyncApiClient
from bi_testing_ya.iam_mock import apply_iam_services_mock

from bi_connector_bundle_ch_frozen.ch_frozen_demo.core.constants import CONNECTION_TYPE_CH_FROZEN_DEMO
from bi_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL
from bi_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL
from bi_connector_oracle.core.constants import CONNECTION_TYPE_ORACLE
from bi_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES

from bi_service_registry_ya_team.yt_service_registry import YTServiceRegistry

from bi_api_lib_tests.app_async import create_app as create_app_async
from bi_api_lib_tests.app_sync import create_app as create_app_sync
from bi_api_lib_tests.utils import recycle_validation_response, get_random_str


pytest_plugins = (
    'aiohttp.pytest_plugin',
    'bi_testing_ya.pytest_plugin',
)
try:
    del aiohttp.pytest_plugin.loop
except AttributeError:
    pass


def pytest_configure(config):  # noqa
    os.environ['ALLOW_SUBQUERY_IN_PREVIEW'] = '1'
    os.environ['NATIVE_WF_POSTGRESQL'] = '1'
    os.environ['NATIVE_WF_MYSQL'] = '1'
    # os.environ['NATIVE_WF_CLICKHOUSE'] = '1'  # FIXME: Implement RANK_PERCENTILE for CLICKHOUSE

    # Need to re-run it so that new factories are registered as a result of the updated os.environ
    register_for_connectors_with_native_wf()  # FIXME: Remove this after removing the NATIVE_WF_* switches

    common_pytest_configure(
        use_jaeger_tracer=env_var_definitions.use_jaeger_tracer(),
        tracing_service_name="tests_bi_api",
    )
    LOGMUTATORS.apply(require=False)
    LOGMUTATORS.add_mutator('ylog_context', add_ylog_context)  # obsolete?


@pytest.fixture(scope='session', autouse=True)
def loaded_libraries() -> None:
    load_bi_api_lib()
    load_bi_db_testing()


@pytest.fixture
def loop(event_loop):
    asyncio.set_event_loop(event_loop)
    yield event_loop
    # Attempt to cover an old version of pytest-asyncio:
    # https://github.com/pytest-dev/pytest-asyncio/commit/51d986cec83fdbc14fa08015424c79397afc7ad9
    asyncio.set_event_loop_policy(None)


@pytest.fixture(scope='session')
def bi_test_config() -> BiApiTestEnvironmentConfiguration:
    return tests_config_mod.BI_TEST_CONFIG


@pytest.fixture(scope='session')
def core_test_config(bi_test_config: BiApiTestEnvironmentConfiguration) -> CoreTestEnvironmentConfigurationBase:
    return bi_test_config.core_test_config


@pytest.fixture(scope='function')
def enable_all_connectors(monkeypatch):
    def _check(conn_type, env_type):    # noqa
        return True
    monkeypatch.setattr(ConnectorAvailabilityConfig, 'check_connector_is_available', _check)


@pytest.fixture(scope='session')
def initdb_ready():
    """ Synchronization fixture that ensures that initdb has finished """
    return wait_for_initdb(
        initdb_host=get_test_container_hostport('init-db', fallback_port=50481).host,
        initdb_port=get_test_container_hostport('init-db', fallback_port=50481).port,
    )


@pytest.fixture(scope='function')
def app(
        bi_test_config: BiApiTestEnvironmentConfiguration,
        initdb_ready,
        rqe_config_subprocess,
        iam_services_mock,
        connectors_settings,
        enable_all_connectors,
        redis_setting_maker,
):
    """Session-wide test `Flask` application."""
    core_test_config = bi_test_config.core_test_config
    us_config = core_test_config.get_us_config()
    settings = ControlPlaneAppSettings(
        ENV_TYPE=EnvType.development,
        APP_TYPE=AppType.TESTS,

        US_BASE_URL=us_config.us_host,
        US_MASTER_TOKEN=us_config.us_master_token,
        CRYPTO_KEYS_CONFIG=core_test_config.get_crypto_keys_config(),

        DLS_HOST=bi_test_config.dls_host,
        DLS_API_KEY=bi_test_config.dls_key,

        YC_AUTH_SETTINGS=YCAuthSettings(
            YC_AS_ENDPOINT=iam_services_mock.service_config.endpoint,
            YC_API_ENDPOINT_IAM=iam_services_mock.service_config.endpoint,
            YC_AUTHORIZE_PERMISSION=None,
        ),
        YC_RM_CP_ENDPOINT=iam_services_mock.service_config.endpoint,
        YC_IAM_TS_ENDPOINT=iam_services_mock.service_config.endpoint,
        CONNECTORS=None,

        RQE_CONFIG=rqe_config_subprocess,
        BI_COMPENG_PG_ON=True,
        BI_COMPENG_PG_URL=bi_test_config.bi_compeng_pg_url,

        DO_DSRC_IDX_FETCH=True,

        FIELD_ID_GENERATOR_TYPE=FieldIdGeneratorType.suffix,

        REDIS_ARQ=redis_setting_maker.get_redis_settings_arq(),

        FILE_UPLOADER_BASE_URL='http://127.0.0.1:9999',  # fake url
        FILE_UPLOADER_MASTER_TOKEN='qwerty',
        MDB=MDBSettings(),
        BLACKBOX_NAME='Test',
    )

    app = create_app_sync(
        settings,
        connectors_settings=connectors_settings,
        testing_app_settings=ControlPlaneAppTestingsSettings(
            fake_tenant=TenantYCFolder(folder_id='folder_1')
        ),
        close_loop_after_request=False,
    )
    app.config['WE_ARE_IN_TESTS'] = True

    # Establish an application context before running the tests.
    with app.app_context() as ctx:
        assert ctx
        yield app


@pytest.fixture(scope='session')
def partner_keys_private_dl():
    return rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )


@pytest.fixture(scope='session')
def partner_keys_private_partner():
    return rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )


@pytest.fixture(scope='session')
def partners_conn_access_token(
        partner_keys_private_dl,
        partner_keys_private_partner,
        clickhouse_db,
):
    data = json.dumps({'db_name': clickhouse_db.get_conn_credentials(full=True)['db_name']})
    public_key_dl = partner_keys_private_dl.public_key()
    ciphertext = public_key_dl.encrypt(data.encode(), padding.PKCS1v15())
    signature = partner_keys_private_partner.sign(ciphertext, padding.PKCS1v15(), hashes.SHA1())
    access_token = ':'.join((
        '1',
        '1',
        b64encode(ciphertext).decode(encoding='utf-8'),
        b64encode(signature).decode(encoding='utf-8'),
    ))
    return access_token


@pytest.fixture(scope='session')
def connectors_settings(clickhouse_db, partner_keys_private_dl, partner_keys_private_partner):
    ch_creds = clickhouse_db.get_conn_credentials(full=True)

    base_settings_params = dict(
        SECURE=False,
        HOST=ch_creds['host'],
        PORT=ch_creds['port'],
        USERNAME=ch_creds['username'],
        PASSWORD=ch_creds['password'],
    )

    sample_ch_frozen_settings = dict(
        USE_MANAGED_NETWORK=False,
        ALLOWED_TABLES=['SampleLite'],
        SUBSELECT_TEMPLATES=(
            {
                'title': 'subselect_1',
                'sql_query': 'select * from SampleLite limit 10',
            },
        ),
        DB_NAME='samples',
        **base_settings_params,
    )
    files3_settings = dict(
        ACCESS_KEY_ID='accessKey1',
        SECRET_ACCESS_KEY='verySecretKey1',
        BUCKET='bi-file-uploader',
        S3_ENDPOINT='http://s3-storage:8000',
        **base_settings_params,
    )

    return {
        CONNECTION_TYPE_CH_FROZEN_DEMO: CHFrozenDemoConnectorSettings(
            PASS_DB_QUERY_TO_USER=True,
            RAW_SQL_LEVEL=RawSQLLevel.dashsql,
            **sample_ch_frozen_settings,
        ),
        CONNECTION_TYPE_FILE: FileS3ConnectorSettings(**files3_settings),
        CONNECTION_TYPE_GSHEETS_V2: FileS3ConnectorSettings(**files3_settings),
        CONNECTION_TYPE_METRICA_API: MetricaConnectorSettings(),
        CONNECTION_TYPE_APPMETRICA_API: AppmetricaConnectorSettings(),
        CONNECTION_TYPE_YDB: YDBConnectorSettings(),
        CONNECTION_TYPE_CHYT: CHYTConnectorSettings(
            FORBIDDEN_CLIQUES=('*ch_public',),
        ),
        CONNECTION_TYPE_CLICKHOUSE: ClickHouseConnectorSettings(),
        CONNECTION_TYPE_GREENPLUM: GreenplumConnectorSettings(),
        CONNECTION_TYPE_MYSQL: MysqlConnectorSettings(),
        CONNECTION_TYPE_POSTGRES: PostgresConnectorSettings(),
        CONNECTION_TYPE_YQ: YQConnectorSettings(
            HOST='grpcs://grpc.yandex-query.cloud-preprod.yandex.net',
            PORT=2135,
            DB_NAME='/root/default',
        ),
    }


def make_sync_services_registry(
        bi_test_config: BiApiTestEnvironmentConfiguration,
        rci: RequestContextInfo,
        async_env: bool,
        rqe_config,
        file_uploader_worker_settings: Optional[FileUploaderWorkerSettings] = None
) -> DefaultBiApiServiceRegistry:
    sr_future_ref: FutureRef[DefaultBiApiServiceRegistry] = FutureRef()
    reporting_registry = DefaultReportingRegistry(rci=rci)

    new_sr = DefaultBiApiServiceRegistry(
        rci=rci,
        reporting_registry=reporting_registry,
        conn_exec_factory=DefaultConnExecutorFactory(
            async_env=async_env,
            services_registry_ref=sr_future_ref,
            rqe_config=rqe_config,
            conn_sec_mgr=InsecureConnectionSecurityManager(),
            mdb_mgr=MDBDomainManagerFactory().get_manager(),
            tpe=None,
        ),
        caches_redis_client_factory=None,  # TODO: should actually probably add one (but with a random key prefix)
        dataset_validator_factory=DefaultDatasetValidatorFactory(),
        mutations_cache_factory=DefaultUSEntryMutationCacheFactory(),
        mdb_domain_manager_factory=MDBDomainManagerFactory(),
        task_processor_factory=LocalTaskProcessorFactory(
            context_fab=FileUploaderContextFab(file_uploader_worker_settings),
            registry=FILE_UPLOADER_WORKER_TASK_REGISTRY,
        ) if file_uploader_worker_settings is not None else DummyTaskProcessorFactory,
        inst_specific_sr=YTServiceRegistry(
            service_registry_ref=sr_future_ref,
            dls_host=bi_test_config.dls_host,
            dls_api_key=bi_test_config.dls_key,
        )
    )
    sr_future_ref.fulfill(new_sr)
    return new_sr


@pytest.fixture(scope='session')
def default_service_registry(
        bi_test_config, bi_context, rqe_config_subprocess, file_uploader_worker_settings
) -> BiApiServiceRegistry:
    sr = make_sync_services_registry(
        bi_test_config, bi_context,
        async_env=False,
        rqe_config=rqe_config_subprocess,
        file_uploader_worker_settings=file_uploader_worker_settings
    )
    yield sr
    sr.close()


@pytest.fixture(scope='session')
def default_sync_usm(bi_context, default_service_registry, core_test_config):
    us_config = core_test_config.get_us_config()
    return SyncUSManager(
        us_base_url=us_config.us_host,
        us_auth_context=USAuthContextMaster(us_config.us_master_token),
        crypto_keys_config=core_test_config.get_crypto_keys_config(),
        bi_context=bi_context,
        services_registry=default_service_registry,
    )


@pytest.fixture(scope='function')
def default_service_registry_per_test(
        bi_test_config, bi_context, rqe_config_subprocess, file_uploader_worker_settings
) -> BiApiServiceRegistry:
    sr = make_sync_services_registry(
        bi_test_config, bi_context,
        async_env=False,
        rqe_config=rqe_config_subprocess,
        file_uploader_worker_settings=file_uploader_worker_settings,
    )
    yield sr
    sr.close()


@pytest.fixture(scope='function')
def default_service_registry_async_env_per_test(
        bi_test_config, bi_context, loop, rqe_config_subprocess, file_uploader_worker_settings
) -> BiApiServiceRegistry:
    sr = make_sync_services_registry(
        bi_test_config, bi_context,
        async_env=True,
        rqe_config=rqe_config_subprocess,
        file_uploader_worker_settings=file_uploader_worker_settings,
    )
    yield sr
    loop.run_until_complete(sr.close_async())


@pytest.fixture(scope='function')
@pytest.mark.usefixtures('loop')
def default_sync_usm_per_test(bi_context, default_service_registry_per_test, core_test_config):
    us_config = core_test_config.get_us_config()
    return SyncUSManager(
        us_base_url=us_config.us_host,
        us_auth_context=USAuthContextMaster(us_config.us_master_token),
        crypto_keys_config=core_test_config.get_crypto_keys_config(),
        bi_context=bi_context,
        services_registry=default_service_registry_per_test,
    )


@pytest.fixture(scope='function')
@pytest.mark.usefixtures('loop')
def default_async_usm_per_test(bi_context, default_service_registry_async_env_per_test, core_test_config):
    us_config = core_test_config.get_us_config()
    return AsyncUSManager(
        us_base_url=us_config.us_host,
        us_auth_context=USAuthContextMaster(us_config.us_master_token),
        crypto_keys_config=core_test_config.get_crypto_keys_config(),
        bi_context=bi_context,
        services_registry=default_service_registry_async_env_per_test,
    )


@pytest.fixture(scope='function')
def client(app, loop):
    from bi_core_testing.flask_utils import FlaskTestResponse, FlaskTestClient

    class TestClient(FlaskTestClient):
        def get_default_headers(self) -> Dict[str, Optional[str]]:
            return {}

    app.test_client_class = TestClient
    app.response_class = FlaskTestResponse  # for the `json` property

    client = app.test_client()

    return client


def _make_dataset(client, connection_id, request, source_type=None, created_via=None, db='test_data', table='sample_superstore'):
    source_id = str(uuid.uuid4())
    if source_type is None:
        source_type = CreateDSFrom.CH_TABLE.name
    body = {
        'updates': [
            {
                'action': 'add_source',
                'source': {
                    'id': source_id,
                    'title': 'test_data',
                    'source_type': source_type,
                    'connection_id': connection_id,
                    'parameters': {
                        'db_name': db,
                        'table_name': table,
                    },
                },
            },
            {
                'action': 'add_source_avatar',
                'source_avatar': {
                    'id': str(uuid.uuid4()),
                    'source_id': source_id,
                    'title': table,
                },
            },
        ],
    }
    if created_via:
        body.update({
            'dataset': {
                'created_via': created_via.name
            },
        })
    response = client.post(
        '/api/v1/datasets/validators/dataset',
        data=json.dumps(body),
        content_type='application/json'
    )
    assert response.status_code == 200, response.json
    result_schema = response.json['dataset']['result_schema']
    for field in result_schema:
        assert field['valid'], f'Field {field["title"]} should be valid'

    data = recycle_validation_response(response.json)
    response = client.post(
        '/api/v1/datasets',
        data=json.dumps(data),
        content_type='application/json'
    )
    assert response.status_code == 200, response.json
    dataset_id = response.json['id']

    def teardown():
        client.delete('/api/v1/datasets/{}'.format(dataset_id))

    request.addfinalizer(teardown)

    return dataset_id


@pytest.fixture(scope='function')
def dataset_id(client, connection_id, request):
    return _make_dataset(client, connection_id, request)


@pytest.fixture(scope='function')
def static_dataset_id(client, static_connection_id, request):
    """Single dataset to be used throughout the test session. It must not be updated!"""
    return _make_dataset(client, static_connection_id, request)


@pytest.fixture(scope='function')
def dynamic_ch_dataset_id(client, dynamic_ch_connection_id, request):
    """ Per-test CH dataset """
    return _make_dataset(client, dynamic_ch_connection_id, request)


@pytest.fixture(scope='function')
def yt_to_dl_dataset_id(client, connection_id, request):
    return _make_dataset(client, connection_id, request, created_via=DataSourceCreatedVia.yt_to_dl)


@pytest.fixture(scope='function')
def api_v1(client):
    return SyncHttpDatasetApiV1(client=FlaskSyncApiClient(int_wclient=client))


@pytest.fixture(scope='session')
def bi_context() -> RequestContextInfo:
    return RequestContextInfo.create(
        request_id=None,
        tenant=TenantCommon(),
        user_name=None,
        user_id='_the_tests_fixture_user_id_',
        x_dl_debug_mode=None,
        secret_headers=None,
        auth_data=None,
        plain_headers=None,
        endpoint_code=None,
        x_dl_context=None,
    )


@pytest.fixture(scope='session')
def rqe_config_subprocess(bi_test_config) -> RQEConfig:
    with RQEConfigurationMaker(bi_test_config=bi_test_config).rqe_config_subprocess_cm() as rqe_config:
        yield rqe_config


@pytest.fixture(scope='session')
def redis_setting_maker(bi_test_config) -> RedisSettingMaker:
    return RedisSettingMaker(bi_test_config=bi_test_config)


@pytest.fixture(scope='function')
def async_app_settings_local_env(
        rqe_config_subprocess, bi_test_config, iam_services_mock,
) -> AsyncAppSettings:
    core_test_config = bi_test_config.core_test_config
    us_config = core_test_config.get_us_config()
    return AsyncAppSettings(
        APP_TYPE=AppType.TESTS,
        PUBLIC_API_KEY=None,
        SENTRY_ENABLED=False,
        US_BASE_URL=us_config.us_host,
        US_MASTER_TOKEN=us_config.us_master_token,
        CRYPTO_KEYS_CONFIG=core_test_config.get_crypto_keys_config(),
        # TODO FIX: Configure caches
        CACHES_ON=False,
        SAMPLES_CH_HOSTS=(),
        RQE_CONFIG=rqe_config_subprocess,

        YC_AUTH_SETTINGS=YCAuthSettings(
            YC_AS_ENDPOINT=iam_services_mock.service_config.endpoint,
            YC_AUTHORIZE_PERMISSION=None,
        ),
        YC_RM_CP_ENDPOINT=iam_services_mock.service_config.endpoint,
        YC_IAM_TS_ENDPOINT=iam_services_mock.service_config.endpoint,

        CONNECTORS=None,
        MUTATIONS_CACHES_ON=False,
        BI_COMPENG_PG_ON=True,
        BI_COMPENG_PG_URL=bi_test_config.bi_compeng_pg_url,
        FIELD_ID_GENERATOR_TYPE=FieldIdGeneratorType.suffix,

        FILE_UPLOADER_BASE_URL='http://127.0.0.1:9999',  # fake url
        FILE_UPLOADER_MASTER_TOKEN='qwerty',

        MDB=MDBSettings(),
    )


@pytest.fixture(scope='function')
def async_app_settings_local_env_with_mutation_cache(
    async_app_settings_local_env, redis_setting_maker
) -> AsyncAppSettings:
    return attr.evolve(async_app_settings_local_env, **dict(
        MUTATIONS_CACHES_ON=True,
        MUTATIONS_REDIS=redis_setting_maker.get_redis_settings_mutation(),
    ))


@pytest.fixture(scope='function')
def async_app_settings_local_env_with_caches(
    async_app_settings_local_env, redis_setting_maker,
) -> AsyncAppSettings:
    cache_redis_settings = redis_setting_maker.get_redis_settings_cache()
    return attr.evolve(async_app_settings_local_env, **dict(
        CACHES_ON=True,
        CACHES_REDIS=cache_redis_settings,
        RQE_CACHES_ON=True,
        RQE_CACHES_REDIS=cache_redis_settings,
    ))


@pytest.fixture(scope='function')
def test_settings_with_bb(tvm_info) -> TestAppSettings:
    return TestAppSettings(
        use_bb_in_test=True,
        tvm_info=tvm_info,
    )


@pytest.fixture(scope='function')
def async_api_local_env_low_level_client(
        loop, aiohttp_client, async_app_settings_local_env, connectors_settings
):
    app = create_app_async(async_app_settings_local_env, connectors_settings)
    return loop.run_until_complete(aiohttp_client(app))


@pytest.fixture(scope='function')
def async_api_local_env_low_level_client_with_mutation_cache(
    loop, aiohttp_client, async_app_settings_local_env_with_mutation_cache, connectors_settings
):
    app = create_app_async(async_app_settings_local_env_with_mutation_cache, connectors_settings)
    return loop.run_until_complete(aiohttp_client(app))


@pytest.fixture(scope='function')
def async_api_local_env_low_level_client_with_bb(
        loop, aiohttp_client, async_app_settings_local_env, test_settings_with_bb, connectors_settings
):
    app = create_app_async(async_app_settings_local_env, connectors_settings, test_settings_with_bb)
    return loop.run_until_complete(aiohttp_client(app))


@pytest.fixture(scope='function')
def async_api_local_env_low_level_client_with_caches(
        loop, aiohttp_client, async_app_settings_local_env_with_caches, connectors_settings
):
    app = create_app_async(async_app_settings_local_env_with_caches, connectors_settings)
    return loop.run_until_complete(aiohttp_client(app))


@pytest.fixture(scope='function')
def flask_sync_client(loop, async_api_local_env_low_level_client) -> WrappedAioSyncApiClient:
    return WrappedAioSyncApiClient(
        int_wrapped_client=TestClientConverterAiohttpToFlask(
            loop=loop,
            aio_client=async_api_local_env_low_level_client,
        ),
    )


@pytest.fixture(scope='function')
def flask_sync_client_with_mutation_cache(loop, async_api_local_env_low_level_client_with_mutation_cache) -> WrappedAioSyncApiClient:
    return WrappedAioSyncApiClient(
        int_wrapped_client=TestClientConverterAiohttpToFlask(
            loop=loop,
            aio_client=async_api_local_env_low_level_client_with_mutation_cache,
        ),
    )


@pytest.fixture(scope='function')
def flask_sync_client_with_caches(loop, async_api_local_env_low_level_client_with_caches) -> WrappedAioSyncApiClient:
    return WrappedAioSyncApiClient(
        int_wrapped_client=TestClientConverterAiohttpToFlask(
            loop=loop,
            aio_client=async_api_local_env_low_level_client_with_caches,
        ),
    )


@pytest.fixture(scope='function')
def data_api_v1(flask_sync_client) -> SyncHttpDataApiV1:
    return SyncHttpDataApiV1(client=flask_sync_client)


@pytest.fixture(scope='function')
def data_api_v1_with_mutation_cache(flask_sync_client_with_mutation_cache) -> SyncHttpDataApiV1:
    return SyncHttpDataApiV1(client=flask_sync_client_with_mutation_cache)


@pytest.fixture(scope='function')
def data_api_v1_with_caches(flask_sync_client_with_caches) -> SyncHttpDataApiV1:
    return SyncHttpDataApiV1(client=flask_sync_client_with_caches)


@pytest.fixture(scope='function', params=[
    lazy_fixture('data_api_v1'),
    lazy_fixture('data_api_v1_with_mutation_cache'),
])
def data_api_v1_test_mutation_cache(request) -> SyncHttpDataApiV1:
    return request.param


@pytest.fixture(scope='function')
def data_api_v1_5(flask_sync_client) -> SyncHttpDataApiV1_5:
    return SyncHttpDataApiV1_5(client=flask_sync_client)


@pytest.fixture(scope='function')
def data_api_v2(flask_sync_client) -> SyncHttpDataApiV2:
    return SyncHttpDataApiV2(client=flask_sync_client)


@pytest.fixture(scope='function')
def data_api_v2_with_mutation_cache(flask_sync_client_with_mutation_cache) -> SyncHttpDataApiV2:
    return SyncHttpDataApiV2(client=flask_sync_client_with_mutation_cache)


@pytest.fixture(scope='function', params=[
    lazy_fixture('data_api_v2'),
    lazy_fixture('data_api_v2_with_mutation_cache')
])
def data_api_v2_test_mutation_cache(request) -> tuple[SyncHttpDataApiV2, bool]:
    """
    Parametrized fixture to run data-api test with mutations caches on and off
    The second return value indicates whether the cache is on
    """

    return request.param, request.param == data_api_v2_test_mutation_cache


def make_data_api_for_v(client: SyncHttpClientBase, v: str) -> Union[SyncHttpDataApiV1, SyncHttpDataApiV1_5, SyncHttpDataApiV2]:
    return {
        'v1': SyncHttpDataApiV1,
        'v1.5': SyncHttpDataApiV1_5,
        'v2': SyncHttpDataApiV2,
    }[v](client=client)


@pytest.fixture(scope='function', params=['v1', 'v1.5'], ids=['v1', 'v1.5'])
def data_api_legacy(request, flask_sync_client) -> Union[SyncHttpDataApiV1, SyncHttpDataApiV1_5]:
    return make_data_api_for_v(client=flask_sync_client, v=request.param)


@pytest.fixture(scope='function', params=['v1', 'v1.5', 'v2'], ids=['v1', 'v1.5', 'v2'])
def data_api_all_v(request, flask_sync_client) -> Union[SyncHttpDataApiV1, SyncHttpDataApiV1_5, SyncHttpDataApiV2]:
    return make_data_api_for_v(client=flask_sync_client, v=request.param)


@pytest.fixture(scope='session')
def us_host(core_test_config):
    us_config = core_test_config.get_us_config()
    return us_config.us_host


class DbParams(NamedTuple):
    host: str
    password: str


# TODO: make into a fixture.
DB_PARAMS = {
    db_key: DbParams(host=host, password=password)
    for db_key, (host, password) in tests_config_mod.DB_PARAMS.items()
}


# TODO: make into a fixture
DB_CONFIGURATIONS = {
    ConnectionType.clickhouse: tests_config_mod.DB_URLS['clickhouse'],
    CONNECTION_TYPE_MSSQL: tests_config_mod.DB_URLS['mssql'],
    CONNECTION_TYPE_MYSQL: tests_config_mod.DB_URLS['mysql'],
    CONNECTION_TYPE_ORACLE: tests_config_mod.DB_URLS['oracle'],
    CONNECTION_TYPE_POSTGRES: tests_config_mod.DB_URLS['postgres'],
}
_SORTED_CONFIGS = sorted(DB_CONFIGURATIONS.items(), key=lambda item: item[0].value)
DEFAULT_SCHEMAS = {
    CONNECTION_TYPE_POSTGRES: 'public',
    CONNECTION_TYPE_MSSQL: 'dbo',
}
SCHEMATIZED_DB = {
    CONNECTION_TYPE_POSTGRES,
    CONNECTION_TYPE_MSSQL,
    CONNECTION_TYPE_ORACLE,
}


@pytest.fixture(scope='session')
def db_dispenser() -> CoreDbDispenser:
    db_dispenser = CoreReInitableDbDispenser()
    db_dispenser.add_reinit_hook(
        db_config=make_db_config(
            conn_type=CONNECTION_TYPE_MSSQL,
            url=tests_config_mod.DB_URLS['mssql'][0],
        ),
        reinit_hook=lambda: restart_container_by_label(label=tests_config_mod.MSSQL_CONTAINER_LABEL,
                                                       compose_project=tests_config_mod.COMPOSE_PROJECT_NAME),
    )
    # tmp workaround to run both in arcadia and gh ci
    # to be removed after complete migration to git from arc
    import sys
    is_arcadia_python = hasattr(sys, "extra_modules")
    if is_arcadia_python:
        db_dispenser.add_reinit_hook(
            db_config=make_db_config(
                conn_type=CONNECTION_TYPE_ORACLE,
                url=tests_config_mod.DB_URLS['oracle'][0],
            ),
            reinit_hook=lambda: restart_container_by_label(label=tests_config_mod.ORACLE_CONTAINER_LABEL,
                                                           compose_project=tests_config_mod.COMPOSE_PROJECT_NAME),
        )
    return db_dispenser


def _db_for(conn_type: ConnectionType, db_dispenser: CoreDbDispenser) -> Optional[Db]:
    url, cluster = DB_CONFIGURATIONS[conn_type]
    if not url:
        return None
    return db_dispenser.get_database(
        db_config=make_db_config(url=url, conn_type=conn_type, cluster=cluster),
    )


def db_for_key(key: str, conn_type: ConnectionType, db_dispenser: CoreDbDispenser) -> Optional[Db]:
    url, cluster = tests_config_mod.DB_URLS[key]
    if not url:
        return None
    return db_dispenser.get_database(
        db_config=make_db_config(url=url, conn_type=conn_type, cluster=cluster),
    )


@pytest.fixture(
    scope='session',
    params=[(url, conn_type) for conn_type, url in _SORTED_CONFIGS],
    ids=[conn_type.value for conn_type, _ in _SORTED_CONFIGS]
)
def any_db(request, initdb_ready, db_dispenser) -> Db:
    url, cluster = request.param[0]
    if not url:
        raise pytest.skip(f'db: No db for {request.param[1].value!r}')
    return db_dispenser.get_database(
        db_config=make_db_config(url=url, conn_type=request.param[1], cluster=cluster),
    )


@pytest.fixture(scope='session')
def any_db_table(any_db):
    """Basic table for all db types"""
    return make_table(any_db, schema=DEFAULT_SCHEMAS.get(any_db.conn_type))


@pytest.fixture(scope='session')
def any_db_table_100(any_db):
    """Basic table for all db types"""
    return make_table(any_db, schema=DEFAULT_SCHEMAS.get(any_db.conn_type), rows=200)


@pytest.fixture(scope='session')
def any_db_two_tables(any_db):
    """Two basic tables for all db types"""
    return (
        make_table(any_db, schema=DEFAULT_SCHEMAS.get(any_db.conn_type)),
        make_table(any_db, schema=DEFAULT_SCHEMAS.get(any_db.conn_type)),
    )


@pytest.fixture(scope='session')
def clickhouse_table_with_arrays(any_db):
    """Basic table for all db types"""
    return make_table_with_arrays(any_db, schema=DEFAULT_SCHEMAS.get(any_db.conn_type))


@pytest.fixture(scope='session')
def clickhouse_db(request, initdb_ready, db_dispenser) -> Db:
    return _db_for(conn_type=ConnectionType.clickhouse, db_dispenser=db_dispenser)


@pytest.fixture(scope='session')
def other_clickhouse_db(request, initdb_ready, db_dispenser) -> Db:
    url, cluster = tests_config_mod.DB_URLS['other_clickhouse']
    return db_dispenser.get_database(
        db_config=make_db_config(url=url, conn_type=ConnectionType.clickhouse, cluster=cluster),
    )


ARRAY_CONN_TYPES = {ConnectionType.clickhouse, CONNECTION_TYPE_POSTGRES}


@pytest.fixture(
    scope='session',
    params=[(url, conn_type) for conn_type, url in _SORTED_CONFIGS if conn_type in ARRAY_CONN_TYPES],
    ids=[conn_type.value for conn_type, _ in _SORTED_CONFIGS if conn_type in ARRAY_CONN_TYPES]
)
def db_with_array_support(request, initdb_ready, db_dispenser) -> Db:
    url, cluster = request.param[0]
    if not url:
        raise pytest.skip(f'db: No db for {request.param[1].value!r}')
    return db_dispenser.get_database(
        db_config=make_db_config(url=url, conn_type=request.param[1], cluster=cluster),
    )


@pytest.fixture(scope='function')
def connection_with_array_support(db_with_array_support, default_sync_usm):
    conn = make_saved_connection(default_sync_usm, db_with_array_support)
    yield conn
    default_sync_usm.delete(conn)


def create_ch_connection(app, client, request, raw_sql_level=None):
    conn_params = ch_connection_params(app)
    if raw_sql_level is not None:
        conn_params.update(raw_sql_level=raw_sql_level)
    resp = client.post(
        '/api/v1/connections',
        data=json.dumps(conn_params),
        content_type='application/json'
    )
    assert resp.status_code == 200, resp.json
    conn_id = resp.json['id']

    def teardown():
        client.delete('/api/v1/connections/{}'.format(conn_id))

    request.addfinalizer(teardown)

    return conn_id


@pytest.fixture(scope='function')
def static_connection_id(app, client, request):  # TODO: Rename to static_ch_connection_id
    """Single connection to be used throughout the whole test session. Must not be updated!"""
    return create_ch_connection(app, client, request)


@pytest.fixture(scope='function')
def dynamic_ch_connection_id(app, client, request):
    """ Per-test CH connection """
    return create_ch_connection(app, client, request)


@pytest.fixture()
def ch_subselectable_connection_id(app, client, request):
    return create_ch_connection(app, client, request, raw_sql_level='dashsql')


def ch_connection_params(app):
    db_params = DB_PARAMS['clickhouse']
    result = {
        'name': 'ch_vla_test_{}'.format(get_random_str()),
        'type': 'clickhouse',
        'host': db_params.host.split(':')[0],
        'port': int(db_params.host.split(':')[1]),
        'username': 'test_user',
        'password': db_params.password,
    }
    return result


def _make_frozen_conn(us_manager: USManagerBase, conn_type: ConnectionType, db: Db) -> ConnectionClickhouseFrozenBase:
    conn_name = f'{conn_type.name} test conn {uuid.uuid4()}'
    conn = ConnectionClickhouseFrozenBase.create_from_dict(
        ConnectionClickhouseFrozenBase.DataModel(**db.get_conn_credentials(full=True)),
        ds_key='connections/tests/{}'.format(conn_name),
        type_=conn_type.name,
        meta={'title': conn_name, 'state': 'saved'},
        us_manager=us_manager,
    )
    return conn


@pytest.fixture(scope='function')
def static_ch_frozen_demo_connection_id(default_sync_usm, clickhouse_db):
    us_manager = default_sync_usm
    conn = _make_frozen_conn(us_manager, CONNECTION_TYPE_CH_FROZEN_DEMO, clickhouse_db)
    us_manager.save(conn)
    return conn.uuid


@pytest.fixture(scope='function')
def any_db_saved_connection(default_sync_usm, any_db, app):
    conn = make_saved_connection(default_sync_usm, any_db)
    yield conn
    default_sync_usm.delete(conn)


@pytest.fixture(scope='session')
def s3_settings() -> S3Settings:
    return S3Settings(
        ENDPOINT_URL=f'http://{get_test_container_hostport("s3-storage", fallback_port=50520).as_pair()}',
        ACCESS_KEY_ID='accessKey1',
        SECRET_ACCESS_KEY='verySecretKey1'
    )


@pytest.fixture(scope='function')
async def s3_bucket(s3_client) -> str:
    bucket_name = 'bi-file-uploader'
    await create_s3_bucket(s3_client, bucket_name)
    yield bucket_name


@pytest.fixture(scope='function')
async def s3_client(s3_settings) -> aiobotocore.client.AioBaseClient:
    async with create_s3_client(s3_settings) as client:
        yield client


@pytest.fixture(scope='session')
def file_uploader_worker_settings(
        redis_setting_maker, s3_settings,
        connectors_settings, core_test_config,
):
    us_config = core_test_config.get_us_config()
    settings = FileUploaderWorkerSettings(
        APP_TYPE=AppType.TESTS,
        REDIS_APP=redis_setting_maker.get_redis_settings_default(),
        REDIS_ARQ=redis_setting_maker.get_redis_settings_arq(),
        S3=S3Settings(
            ENDPOINT_URL=s3_settings.ENDPOINT_URL,
            ACCESS_KEY_ID=s3_settings.ACCESS_KEY_ID,
            SECRET_ACCESS_KEY=s3_settings.SECRET_ACCESS_KEY,
        ),
        S3_TMP_BUCKET_NAME='bi-file-uploader-tmp',
        S3_PERSISTENT_BUCKET_NAME='bi-file-uploader',
        SENTRY_DSN=None,
        US_BASE_URL=us_config.us_host,
        US_MASTER_TOKEN=us_config.us_master_token,
        CONNECTORS=connectors_settings,
        GSHEETS_APP=GoogleAppSettings(
            API_KEY='dummy',
            CLIENT_ID='dummy',
            CLIENT_SECRET='dummy',
        ),
        CRYPTO_KEYS_CONFIG=get_dummy_crypto_keys_config(),
    )
    yield settings


@pytest.fixture()
def iam_services_mock(monkeypatch):
    yield from apply_iam_services_mock(monkeypatch)
