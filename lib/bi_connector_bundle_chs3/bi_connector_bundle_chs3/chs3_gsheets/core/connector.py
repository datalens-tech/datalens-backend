from bi_constants.enums import ConnectionType, CreateDSFrom

from bi_core.connectors.clickhouse_base.adapters import ClickHouseAdapter
from bi_connector_bundle_chs3.chs3_base.core.connector import (
    BaseFileS3CoreConnectionDefinition,
    BaseFileS3TableCoreSourceDefinition,
    BaseFileS3CoreConnector,
)
from bi_connector_bundle_chs3.chs3_gsheets.core.adapter import AsyncGSheetsFileS3Adapter
from bi_connector_bundle_chs3.chs3_gsheets.core.connection_executors import GSheetsFileS3AsyncAdapterConnExecutor
from bi_connector_bundle_chs3.chs3_gsheets.core.data_source import GSheetsFileS3DataSource
from bi_connector_bundle_chs3.chs3_gsheets.core.data_source_spec import (
    GSheetsFileS3DataSourceSpec,
)
from bi_connector_bundle_chs3.chs3_gsheets.core.lifecycle import GSheetsFileS3ConnectionLifecycleManager
from bi_connector_bundle_chs3.chs3_gsheets.core.storage_schemas.connection import GSheetsFileConnectionDataStorageSchema
from bi_connector_bundle_chs3.chs3_gsheets.core.storage_schemas.data_source_spec import GSheetsFileS3DataSourceSpecStorageSchema
from bi_connector_bundle_chs3.chs3_gsheets.core.us_connection import GSheetsFileS3Connection


class GSheetsFileS3CoreConnectionDefinition(BaseFileS3CoreConnectionDefinition):
    conn_type = ConnectionType.gsheets_v2
    connection_cls = GSheetsFileS3Connection
    us_storage_schema_cls = GSheetsFileConnectionDataStorageSchema
    sync_conn_executor_cls = GSheetsFileS3AsyncAdapterConnExecutor
    async_conn_executor_cls = GSheetsFileS3AsyncAdapterConnExecutor
    lifecycle_manager_cls = GSheetsFileS3ConnectionLifecycleManager
    dialect_string = 'bi_clickhouse'


class GSheetsFileS3TableCoreSourceDefinition(BaseFileS3TableCoreSourceDefinition):
    source_type = CreateDSFrom.GSHEETS_V2
    source_cls = GSheetsFileS3DataSource
    source_spec_cls = GSheetsFileS3DataSourceSpec
    us_storage_schema_cls = GSheetsFileS3DataSourceSpecStorageSchema


class GSheetsFileS3CoreConnector(BaseFileS3CoreConnector):
    connection_definitions = (
        GSheetsFileS3CoreConnectionDefinition,
    )
    source_definitions = (
        GSheetsFileS3TableCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({ClickHouseAdapter, AsyncGSheetsFileS3Adapter})
