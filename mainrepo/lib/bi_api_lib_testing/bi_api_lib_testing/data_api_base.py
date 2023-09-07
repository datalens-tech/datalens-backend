import abc
import asyncio
from http import HTTPStatus
from typing import ClassVar, Generator, Iterable, NamedTuple

from aiohttp import web
from aiohttp.pytest_plugin import AiohttpClient
from aiohttp.test_utils import TestClient
import pytest

from bi_configs.enums import AppType
from bi_configs.rqe import RQEConfig
from bi_configs.connectors_settings import ConnectorSettingsBase
from bi_constants.enums import ConnectionType

from bi_cloud_integration.iam_mock import IAMServicesMockFacade

from bi_core.components.ids import FieldIdGeneratorType

from bi_api_lib.app_settings import AsyncAppSettings, YCAuthSettings, MDBSettings
from bi_api_lib.loader import ApiLibraryConfig, preload_bi_api_lib, load_bi_api_lib

from bi_api_client.dsmaker.primitives import Dataset
from bi_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from bi_api_client.dsmaker.api.data_api import SyncHttpDataApiV2, HttpDataApiResponse

from bi_api_lib_testing.app import RedisSettingMaker, TestingDataApiAppFactory
from bi_api_lib_testing.base import BiApiTestBase
from bi_api_lib_testing.configuration import BiApiTestEnvironmentConfiguration
from bi_api_lib_testing.client import TestClientConverterAiohttpToFlask, WrappedAioSyncApiClient
from bi_api_lib_testing.dataset_base import DatasetTestBase
from bi_core_testing.database import DbTable


class DataApiTestParams(NamedTuple):
    two_dims: tuple[str, str]
    summable_field: str
    range_field: str
    distinct_field: str
    date_field: str


class DataApiTestBase(BiApiTestBase, metaclass=abc.ABCMeta):
    mutation_caches_on: ClassVar[bool] = True
    data_caches_on: ClassVar[bool] = True

    @pytest.fixture
    def loop(self, event_loop: asyncio.AbstractEventLoop) -> Generator[asyncio.AbstractEventLoop, None, None]:
        asyncio.set_event_loop(event_loop)
        yield event_loop
        # Attempt to cover an old version of pytest-asyncio:
        # https://github.com/pytest-dev/pytest-asyncio/commit/51d986cec83fdbc14fa08015424c79397afc7ad9
        asyncio.set_event_loop_policy(None)

    @pytest.fixture(scope='function')
    def data_api_app_settings(
            self,
            bi_test_config: BiApiTestEnvironmentConfiguration,
            rqe_config_subprocess: RQEConfig,
            iam_services_mock: IAMServicesMockFacade,
    ) -> AsyncAppSettings:  # TODO SPLIT switch to proper settings
        preload_bi_api_lib()
        core_test_config = bi_test_config.core_test_config
        us_config = core_test_config.get_us_config()
        redis_setting_maker = RedisSettingMaker(bi_test_config=bi_test_config)

        return AsyncAppSettings(
            APP_TYPE=AppType.TESTS,
            PUBLIC_API_KEY=None,
            SENTRY_ENABLED=False,
            US_BASE_URL=us_config.us_host,
            US_MASTER_TOKEN=us_config.us_master_token,
            CRYPTO_KEYS_CONFIG=core_test_config.get_crypto_keys_config(),
            # TODO FIX: Configure caches
            CACHES_ON=self.data_caches_on,
            SAMPLES_CH_HOSTS=(),
            RQE_CONFIG=rqe_config_subprocess,

            YC_AUTH_SETTINGS=YCAuthSettings(
                YC_AS_ENDPOINT=iam_services_mock.service_config.endpoint,
                YC_API_ENDPOINT_IAM=iam_services_mock.service_config.endpoint,
                YC_AUTHORIZE_PERMISSION=None,
            ),  # type: ignore
            YC_RM_CP_ENDPOINT=iam_services_mock.service_config.endpoint,
            YC_IAM_TS_ENDPOINT=iam_services_mock.service_config.endpoint,

            CONNECTOR_WHITELIST=tuple(bi_test_config.connector_whitelist.split(',')),  # FIXME: make a separate classvar
            MUTATIONS_CACHES_ON=self.mutation_caches_on,
            CACHES_REDIS=redis_setting_maker.get_redis_settings_cache(),
            BI_COMPENG_PG_ON=self.bi_compeng_pg_on,
            BI_COMPENG_PG_URL=bi_test_config.bi_compeng_pg_url,
            FIELD_ID_GENERATOR_TYPE=FieldIdGeneratorType.suffix,

            FILE_UPLOADER_BASE_URL='http://127.0.0.1:9999',  # fake url
            FILE_UPLOADER_MASTER_TOKEN='qwerty',

            MDB=MDBSettings(),
        )  # type: ignore

    @pytest.fixture(scope='function')
    def data_api_app(
            self, data_api_app_settings: AsyncAppSettings,
            connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
    ) -> web.Application:
        data_api_app_factory = TestingDataApiAppFactory(settings=data_api_app_settings)
        load_bi_api_lib(ApiLibraryConfig(api_connector_ep_names=data_api_app_settings.CONNECTOR_WHITELIST))
        return data_api_app_factory.create_app(
            connectors_settings=connectors_settings,
            test_setting=None,
        )

    @pytest.fixture(scope='function')
    def data_api_lowlevel_aiohttp_client(
            self, loop: asyncio.AbstractEventLoop, aiohttp_client: AiohttpClient, data_api_app: web.Application,
    ) -> TestClient:
        return loop.run_until_complete(aiohttp_client(data_api_app))

    @pytest.fixture(scope='function')
    def data_api_sync_client(
            self, loop: asyncio.AbstractEventLoop, data_api_lowlevel_aiohttp_client: TestClient,
    ) -> WrappedAioSyncApiClient:
        return WrappedAioSyncApiClient(
            int_wrapped_client=TestClientConverterAiohttpToFlask(
                loop=loop,
                aio_client=data_api_lowlevel_aiohttp_client,
            ),
        )

    @pytest.fixture(scope='function')
    def data_api(self, data_api_sync_client: SyncHttpClientBase) -> SyncHttpDataApiV2:
        return SyncHttpDataApiV2(client=data_api_sync_client)


class StandardizedDataApiTestBase(DataApiTestBase, DatasetTestBase, metaclass=abc.ABCMeta):
    @pytest.fixture(scope='class')
    def data_api_test_params(self) -> DataApiTestParams:
        # This default is defined for the sample table
        return DataApiTestParams(
            two_dims=('category', 'city'),
            summable_field='sales',
            range_field='sales',
            distinct_field='city',
            date_field='order_date',
        )

    def get_dataset_params(self, dataset_params: dict, db_table: DbTable) -> dict:
        return dataset_params | dict(
            parameters=dataset_params.get('parameters', {}) | dict(
                schema_name=db_table.schema,
                table_name=db_table.name,
            )
        )

    def get_result(
            self, ds: Dataset, data_api: SyncHttpDataApiV2, field_names: Iterable[str],
    ) -> HttpDataApiResponse:
        data_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title=field_name) for field_name in field_names],
        )
        assert data_resp.status_code == HTTPStatus.OK, data_resp.response_errors
        return data_resp

    def get_range(
            self, ds: Dataset, data_api: SyncHttpDataApiV2, field_name: str,
    ) -> HttpDataApiResponse:
        data_resp = data_api.get_value_range(
            dataset=ds, field=ds.find_field(title=field_name),
        )
        assert data_resp.status_code == HTTPStatus.OK, data_resp.response_errors
        return data_resp

    def get_distinct(
            self, ds: Dataset, data_api: SyncHttpDataApiV2, field_name: str,
    ) -> HttpDataApiResponse:
        data_resp = data_api.get_distinct(
            dataset=ds, field=ds.find_field(title=field_name),
        )
        assert data_resp.status_code == HTTPStatus.OK, data_resp.response_errors
        return data_resp

    def get_result_ordered(
            self, ds: Dataset, data_api: SyncHttpDataApiV2, field_names: Iterable[str], order_by: Iterable[str],
    ) -> HttpDataApiResponse:
        data_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title=field_name) for field_name in field_names],
            order_by=[ds.find_field(title=field_name) for field_name in order_by]
        )
        assert data_resp.status_code == HTTPStatus.OK, data_resp.response_errors
        return data_resp

    def get_preview(self, ds: Dataset, data_api: SyncHttpDataApiV2) -> HttpDataApiResponse:
        data_resp = data_api.get_preview(dataset=ds)
        assert data_resp.status_code == HTTPStatus.OK, data_resp.response_errors
        return data_resp
