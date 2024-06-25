from __future__ import annotations

import abc
import asyncio
from typing import (
    TYPE_CHECKING,
    AsyncGenerator,
    ClassVar,
    Generator,
    TypeVar,
)
import uuid

import attr
import pytest

from dl_api_commons.base_models import (
    RequestContextInfo,
    TenantCommon,
)
from dl_configs.settings_submodels import S3Settings
from dl_constants.enums import DataSourceType
from dl_core.db import (
    SchemaColumn,
    get_type_transformer,
)
from dl_core.services_registry import ServicesRegistry
from dl_core_testing.configuration import RedisSettingMaker
from dl_core_testing.database import DbTable
from dl_core_testing.fixtures.primitives import FixtureTableSpec
from dl_core_testing.fixtures.sample_tables import TABLE_SPEC_SAMPLE_SUPERSTORE
from dl_core_testing.testcases.connection import BaseConnectionTestClass
from dl_file_uploader_worker_lib.utils.parsing_utils import get_field_id_generator
from dl_task_processor.processor import (
    DummyTaskProcessorFactory,
    TaskProcessorFactory,
)
from dl_testing.s3_utils import (
    create_s3_bucket,
    create_s3_client,
)

from dl_connector_bundle_chs3.chs3_base.core.testing.utils import create_s3_native_from_ch_table
from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from dl_connector_bundle_chs3_tests.db import config as test_config
from dl_connector_clickhouse.db_testing.engine_wrapper import ClickhouseDbEngineConfig


if TYPE_CHECKING:
    from types_aiobotocore_s3 import S3Client as AsyncS3Client


FILE_CONN_TV = TypeVar("FILE_CONN_TV", bound=BaseFileS3Connection)


class BaseCHS3TestClass(BaseConnectionTestClass[FILE_CONN_TV], metaclass=abc.ABCMeta):
    core_test_config = test_config.CORE_TEST_CONFIG
    connection_settings = test_config.SR_CONNECTION_SETTINGS

    source_type: ClassVar[DataSourceType]

    @pytest.fixture(scope="class")
    def event_loop(self):
        """Avoid spontaneous event loop closes between tests"""
        loop = asyncio.get_event_loop()
        yield loop
        loop.close()

    @pytest.fixture(scope="function", autouse=True)
    # FIXME: This fixture is a temporary solution for failing core tests when they are run together with api tests
    def loop(self, event_loop: asyncio.AbstractEventLoop) -> Generator[asyncio.AbstractEventLoop, None, None]:
        asyncio.set_event_loop(event_loop)
        yield event_loop
        # Attempt to cover an old version of pytest-asyncio:
        # https://github.com/pytest-dev/pytest-asyncio/commit/51d986cec83fdbc14fa08015424c79397afc7ad9
        asyncio.set_event_loop_policy(None)

    @pytest.fixture(scope="class")
    def db_url(self) -> str:
        return test_config.DB_CH_URL

    @pytest.fixture(scope="class")
    def engine_config(self, db_url: str, engine_params: dict) -> ClickhouseDbEngineConfig:
        return ClickhouseDbEngineConfig(url=db_url, engine_params=engine_params)

    @pytest.fixture(scope="class")
    def conn_bi_context(self) -> RequestContextInfo:
        return RequestContextInfo(tenant=TenantCommon())

    @pytest.fixture(scope="class")
    def redis_setting_maker(self) -> RedisSettingMaker:
        core_test_config = test_config.CORE_TEST_CONFIG
        return core_test_config.get_redis_setting_maker()

    @pytest.fixture(scope="class")
    def s3_settings(self) -> S3Settings:
        return S3Settings(
            ENDPOINT_URL=test_config.S3_ENDPOINT_URL,
            ACCESS_KEY_ID=self.connection_settings.ACCESS_KEY_ID,
            SECRET_ACCESS_KEY=self.connection_settings.SECRET_ACCESS_KEY,
        )

    @pytest.fixture(scope="class")
    def task_processor_factory(self) -> TaskProcessorFactory:
        return DummyTaskProcessorFactory()

    @pytest.fixture(scope="class")
    def conn_sync_service_registry(
        self,
        root_certificates: bytes,
        conn_bi_context: RequestContextInfo,
        task_processor_factory: TaskProcessorFactory,
    ) -> ServicesRegistry:
        return self.service_registry_factory(
            conn_exec_factory_async_env=False,
            conn_bi_context=conn_bi_context,
            task_processor_factory=task_processor_factory,
            root_certificates_data=root_certificates,
        )

    @pytest.fixture(scope="class")
    def conn_async_service_registry(
        self,
        root_certificates: bytes,
        conn_bi_context: RequestContextInfo,
        task_processor_factory: TaskProcessorFactory,
    ) -> ServicesRegistry:
        return self.service_registry_factory(
            conn_exec_factory_async_env=True,
            conn_bi_context=conn_bi_context,
            task_processor_factory=task_processor_factory,
            root_certificates_data=root_certificates,
        )

    @pytest.fixture(scope="function")
    async def s3_client(self, s3_settings: S3Settings) -> AsyncS3Client:
        async with create_s3_client(s3_settings) as client:
            yield client

    @pytest.fixture(scope="function")
    async def s3_bucket(self, s3_client: AsyncS3Client) -> str:
        bucket_name = self.connection_settings.BUCKET
        await create_s3_bucket(s3_client, bucket_name)
        return bucket_name

    @pytest.fixture(scope="class")
    def sample_table_spec(self) -> FixtureTableSpec:
        return attr.evolve(TABLE_SPEC_SAMPLE_SUPERSTORE, nullable=True)

    def _get_s3_func_schema_for_table(self, table: DbTable) -> str:
        field_id_gen = get_field_id_generator(self.conn_type)
        tbl_schema = ", ".join(
            "{} {}".format(field_id_gen.make_field_id(dict(title=col.name, index=idx)), col.type.compile())
            for idx, col in enumerate(table.table.columns)
        )
        tbl_schema = tbl_schema.replace("()", "")  # String() -> String: type arguments are not needed here
        return tbl_schema

    @pytest.fixture(scope="function")
    async def sample_s3_file(
        self,
        s3_client: AsyncS3Client,
        s3_bucket: str,
        s3_settings: S3Settings,
        sample_table: DbTable,
    ) -> AsyncGenerator[str, None]:
        filename = f"my_file_{uuid.uuid4()}.native"

        tbl_schema = self._get_s3_func_schema_for_table(sample_table)
        create_s3_native_from_ch_table(filename, s3_bucket, s3_settings, sample_table, tbl_schema)

        yield filename

        await s3_client.delete_object(Bucket=s3_bucket, Key=filename)

    def _get_raw_schema_for_ch_table(self, table_spec: FixtureTableSpec) -> list[SchemaColumn]:
        field_id_gen = get_field_id_generator(self.conn_type)
        type_transformer = get_type_transformer(self.conn_type)
        raw_schema = [
            SchemaColumn(
                name=field_id_gen.make_field_id(dict(title=col[0], index=idx)),
                title=col[0],
                user_type=(user_type := table_spec.get_user_type_for_col(col[0])),
                native_type=type_transformer.type_user_to_native(user_type),
            )
            for idx, col in enumerate(table_spec.table_schema)
        ]
        return raw_schema

    @abc.abstractmethod
    @pytest.fixture(scope="function")
    def sample_file_data_source(self, sample_s3_file: str) -> BaseFileS3Connection.FileDataSource:
        raise NotImplementedError()

    @pytest.fixture(scope="function")
    def connection_creation_params(self, sample_file_data_source: BaseFileS3Connection.FileDataSource) -> dict:
        return dict(sources=[sample_file_data_source])
