import abc
import asyncio
from http import HTTPStatus
from typing import (
    ClassVar,
    Generator,
    Iterable,
    NamedTuple,
    Optional,
)

from aiohttp import web
from aiohttp.pytest_plugin import AiohttpClient
from aiohttp.test_utils import TestClient
import pytest

from dl_api_client.dsmaker.api.data_api import (
    HttpDataApiResponse,
    SyncHttpDataApiV2,
)
from dl_api_client.dsmaker.api.http_sync_base import SyncHttpClientBase
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib.app.data_api.app import DataApiAppFactory
from dl_api_lib.app_settings import DataApiAppSettings
from dl_api_lib_testing.app import TestingDataApiAppFactory
from dl_api_lib_testing.base import ApiTestBase
from dl_api_lib_testing.client import (
    TestClientConverterAiohttpToFlask,
    WrappedAioSyncApiClient,
)
from dl_api_lib_testing.configuration import ApiTestEnvironmentConfiguration
from dl_api_lib_testing.dataset_base import DatasetTestBase
from dl_configs.connectors_settings import ConnectorSettingsBase
from dl_configs.rqe import RQEConfig
from dl_constants.enums import ConnectionType
from dl_core.components.ids import FieldIdGeneratorType
from dl_core_testing.database import DbTable
from dl_pivot_pandas.pandas.constants import PIVOT_ENGINE_TYPE_PANDAS
from dl_testing.utils import get_root_certificates_path


class DataApiTestParams(NamedTuple):
    two_dims: tuple[str, str]
    summable_field: str
    range_field: str
    distinct_field: str
    date_field: str


class DataApiTestBase(ApiTestBase, metaclass=abc.ABCMeta):
    mutation_caches_enabled: ClassVar[bool] = True
    data_caches_enabled: ClassVar[bool] = True

    @pytest.fixture
    def loop(self, event_loop: asyncio.AbstractEventLoop) -> Generator[asyncio.AbstractEventLoop, None, None]:
        asyncio.set_event_loop(event_loop)
        yield event_loop
        # Attempt to cover an old version of pytest-asyncio:
        # https://github.com/pytest-dev/pytest-asyncio/commit/51d986cec83fdbc14fa08015424c79397afc7ad9
        asyncio.set_event_loop_policy(None)

    @classmethod
    def create_data_api_settings(
        cls,
        bi_test_config: ApiTestEnvironmentConfiguration,
        rqe_config_subprocess: RQEConfig,
    ) -> DataApiAppSettings:
        core_test_config = bi_test_config.core_test_config
        us_config = core_test_config.get_us_config()
        redis_setting_maker = core_test_config.get_redis_setting_maker()

        return DataApiAppSettings(  # type: ignore  # 2024-01-30 # TODO: Unexpected keyword argument "SENTRY_ENABLED" for "DataApiAppSettings"  [call-arg]
            SENTRY_ENABLED=False,
            US_BASE_URL=us_config.us_host,
            US_MASTER_TOKEN=us_config.us_master_token,
            CRYPTO_KEYS_CONFIG=core_test_config.get_crypto_keys_config(),
            # TODO FIX: Configure caches
            CACHES_ON=cls.data_caches_enabled,
            SAMPLES_CH_HOSTS=(),
            RQE_CONFIG=rqe_config_subprocess,
            BI_API_CONNECTOR_WHITELIST=bi_test_config.get_api_library_config().api_connector_ep_names,
            CORE_CONNECTOR_WHITELIST=core_test_config.get_core_library_config().core_connector_ep_names,
            MUTATIONS_CACHES_ON=cls.mutation_caches_enabled,
            CACHES_REDIS=redis_setting_maker.get_redis_settings_cache(),
            BI_COMPENG_PG_ON=cls.compeng_enabled,
            BI_COMPENG_PG_URL=core_test_config.get_compeng_url(),
            FIELD_ID_GENERATOR_TYPE=FieldIdGeneratorType.suffix,
            FILE_UPLOADER_BASE_URL=f"{bi_test_config.file_uploader_api_host}:{bi_test_config.file_uploader_api_port}",
            FILE_UPLOADER_MASTER_TOKEN="qwerty",
            QUERY_PROCESSING_MODE=cls.query_processing_mode,
            CA_FILE_PATH=get_root_certificates_path(),
            PIVOT_ENGINE_TYPE=PIVOT_ENGINE_TYPE_PANDAS,
        )

    @pytest.fixture(scope="function")
    def data_api_app_settings(
        self,
        bi_test_config: ApiTestEnvironmentConfiguration,
        rqe_config_subprocess: RQEConfig,
    ) -> DataApiAppSettings:
        return self.create_data_api_settings(
            bi_test_config=bi_test_config,
            rqe_config_subprocess=rqe_config_subprocess,
        )

    @pytest.fixture(scope="function")
    def data_api_app_factory(self, data_api_app_settings: DataApiAppSettings) -> DataApiAppFactory:
        return TestingDataApiAppFactory(settings=data_api_app_settings)

    @pytest.fixture(scope="function")
    def data_api_app(
        self,
        data_api_app_factory: DataApiAppFactory,
        connectors_settings: dict[ConnectionType, ConnectorSettingsBase],
    ) -> web.Application:
        return data_api_app_factory.create_app(
            connectors_settings=connectors_settings,
        )

    @pytest.fixture(scope="function")
    def data_api_lowlevel_aiohttp_client(
        self,
        loop: asyncio.AbstractEventLoop,
        aiohttp_client: AiohttpClient,
        data_api_app: web.Application,
    ) -> TestClient:
        return loop.run_until_complete(aiohttp_client(data_api_app))

    @pytest.fixture(scope="function")
    def data_api_sync_client(
        self,
        loop: asyncio.AbstractEventLoop,
        data_api_lowlevel_aiohttp_client: TestClient,
    ) -> WrappedAioSyncApiClient:
        return WrappedAioSyncApiClient(
            int_wrapped_client=TestClientConverterAiohttpToFlask(
                loop=loop,
                aio_client=data_api_lowlevel_aiohttp_client,
            ),
        )

    @pytest.fixture(scope="function")
    def data_api(
        self,
        data_api_sync_client: SyncHttpClientBase,
        bi_headers: Optional[dict[str, str]],
    ) -> SyncHttpDataApiV2:
        return SyncHttpDataApiV2(client=data_api_sync_client, headers=bi_headers or {})


class StandardizedDataApiTestBase(DataApiTestBase, DatasetTestBase, metaclass=abc.ABCMeta):
    @pytest.fixture(scope="class")
    def data_api_test_params(self) -> DataApiTestParams:
        # This default is defined for the sample table
        return DataApiTestParams(
            two_dims=("category", "city"),
            summable_field="sales",
            range_field="sales",
            distinct_field="city",
            date_field="order_date",
        )

    @pytest.fixture(scope="class")
    def table_schema_name(self) -> Optional[str]:
        return None

    def get_dataset_params(self, dataset_params: dict, db_table: DbTable) -> dict:
        return dataset_params | dict(
            parameters=dataset_params.get("parameters", {})
            | dict(
                schema_name=db_table.schema,
                table_name=db_table.name,
            )
        )

    def get_result(
        self,
        ds: Dataset,
        data_api: SyncHttpDataApiV2,
        field_names: Iterable[str],
    ) -> HttpDataApiResponse:
        data_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title=field_name) for field_name in field_names],
        )
        assert data_resp.status_code == HTTPStatus.OK, data_resp.response_errors
        return data_resp

    def get_range(
        self,
        ds: Dataset,
        data_api: SyncHttpDataApiV2,
        field_name: str,
    ) -> HttpDataApiResponse:
        data_resp = data_api.get_value_range(
            dataset=ds,
            field=ds.find_field(title=field_name),
        )
        assert data_resp.status_code == HTTPStatus.OK, data_resp.response_errors
        return data_resp

    def get_distinct(
        self,
        ds: Dataset,
        data_api: SyncHttpDataApiV2,
        field_name: str,
    ) -> HttpDataApiResponse:
        data_resp = data_api.get_distinct(
            dataset=ds,
            field=ds.find_field(title=field_name),
        )
        assert data_resp.status_code == HTTPStatus.OK, data_resp.response_errors
        return data_resp

    def get_result_ordered(
        self,
        ds: Dataset,
        data_api: SyncHttpDataApiV2,
        field_names: Iterable[str],
        order_by: Iterable[str],
    ) -> HttpDataApiResponse:
        data_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title=field_name) for field_name in field_names],
            order_by=[ds.find_field(title=field_name) for field_name in order_by],
        )
        assert data_resp.status_code == HTTPStatus.OK, data_resp.response_errors
        return data_resp

    def get_preview(self, ds: Dataset, data_api: SyncHttpDataApiV2, limit: Optional[int] = None) -> HttpDataApiResponse:
        if limit:
            data_resp = data_api.get_preview(dataset=ds, limit=limit)
        else:
            data_resp = data_api.get_preview(dataset=ds)
        assert data_resp.status_code == HTTPStatus.OK, data_resp.response_errors
        return data_resp
