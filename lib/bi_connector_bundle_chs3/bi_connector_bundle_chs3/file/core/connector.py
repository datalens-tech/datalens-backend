from bi_core.connectors.clickhouse_base.adapters import ClickHouseAdapter

from bi_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE, SOURCE_TYPE_FILE_S3_TABLE
from bi_connector_bundle_chs3.chs3_base.core.connector import (
    BaseFileS3CoreConnectionDefinition,
    BaseFileS3TableCoreSourceDefinition,
    BaseFileS3CoreConnector,
)
from bi_connector_bundle_chs3.file.core.adapter import AsyncFileS3Adapter
from bi_connector_bundle_chs3.file.core.connection_executors import FileS3AsyncAdapterConnExecutor
from bi_connector_bundle_chs3.file.core.data_source import FileS3DataSource
from bi_connector_bundle_chs3.file.core.data_source_spec import FileS3DataSourceSpec
from bi_connector_bundle_chs3.file.core.lifecycle import FileS3ConnectionLifecycleManager
from bi_connector_bundle_chs3.file.core.settings import FileS3SettingDefinition
from bi_connector_bundle_chs3.file.core.storage_schemas.connection import FileConnectionDataStorageSchema
from bi_connector_bundle_chs3.file.core.storage_schemas.data_source_spec import FileS3DataSourceSpecStorageSchema
from bi_connector_bundle_chs3.file.core.us_connection import FileS3Connection


class FileS3CoreConnectionDefinition(BaseFileS3CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_FILE
    connection_cls = FileS3Connection
    us_storage_schema_cls = FileConnectionDataStorageSchema
    sync_conn_executor_cls = FileS3AsyncAdapterConnExecutor
    async_conn_executor_cls = FileS3AsyncAdapterConnExecutor
    lifecycle_manager_cls = FileS3ConnectionLifecycleManager
    dialect_string = 'bi_clickhouse'
    settings_definition = FileS3SettingDefinition


class FileS3TableCoreSourceDefinition(BaseFileS3TableCoreSourceDefinition):
    source_type = SOURCE_TYPE_FILE_S3_TABLE
    source_cls = FileS3DataSource
    source_spec_cls = FileS3DataSourceSpec
    us_storage_schema_cls = FileS3DataSourceSpecStorageSchema


class FileS3CoreConnector(BaseFileS3CoreConnector):
    connection_definitions = (
        FileS3CoreConnectionDefinition,
    )
    source_definitions = (
        FileS3TableCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({ClickHouseAdapter, AsyncFileS3Adapter})
