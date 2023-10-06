from clickhouse_sqlalchemy.orm.query import Query as CHQuery

from dl_core.connectors.base.connector import (
    CoreConnectionDefinition,
    CoreConnector,
    CoreSourceDefinition,
)

from dl_connector_chyt.core.adapters import CHYTAdapter
from dl_connector_chyt.core.async_adapters import AsyncCHYTAdapter
from dl_connector_chyt.core.connection_executors import (
    CHYTAsyncAdapterConnExecutor,
    CHYTSyncAdapterConnExecutor,
)
from dl_connector_chyt.core.constants import (
    BACKEND_TYPE_CHYT,
    CONNECTION_TYPE_CHYT,
    SOURCE_TYPE_CHYT_YTSAURUS_SUBSELECT,
    SOURCE_TYPE_CHYT_YTSAURUS_TABLE,
    SOURCE_TYPE_CHYT_YTSAURUS_TABLE_LIST,
    SOURCE_TYPE_CHYT_YTSAURUS_TABLE_RANGE,
)
from dl_connector_chyt.core.data_source import (
    CHYTTableDataSource,
    CHYTTableListDataSource,
    CHYTTableRangeDataSource,
    CHYTTableSubselectDataSource,
)
from dl_connector_chyt.core.data_source_migration import CHYTDataSourceMigrator
from dl_connector_chyt.core.data_source_spec import (
    CHYTSubselectDataSourceSpec,
    CHYTTableDataSourceSpec,
    CHYTTableListDataSourceSpec,
    CHYTTableRangeDataSourceSpec,
)
from dl_connector_chyt.core.settings import CHYTSettingDefinition
from dl_connector_chyt.core.storage_schemas.connection import ConnectionCHYTDataStorageSchema
from dl_connector_chyt.core.storage_schemas.data_source_spec import (
    CHYTSubselectDataSourceSpecStorageSchema,
    CHYTTableDataSourceSpecStorageSchema,
    CHYTTableListDataSourceSpecStorageSchema,
    CHYTTableRangeDataSourceSpecStorageSchema,
)
from dl_connector_chyt.core.type_transformer import CHYTTypeTransformer
from dl_connector_chyt.core.us_connection import ConnectionCHYTToken


class CHYTCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_CHYT
    connection_cls = ConnectionCHYTToken
    us_storage_schema_cls = ConnectionCHYTDataStorageSchema
    type_transformer_cls = CHYTTypeTransformer
    sync_conn_executor_cls = CHYTSyncAdapterConnExecutor
    async_conn_executor_cls = CHYTAsyncAdapterConnExecutor
    dialect_string = "bi_chyt"
    settings_definition = CHYTSettingDefinition
    data_source_migrator_cls = CHYTDataSourceMigrator


class CHYTTableCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CHYT_YTSAURUS_TABLE
    source_cls = CHYTTableDataSource
    source_spec_cls = CHYTTableDataSourceSpec
    us_storage_schema_cls = CHYTTableDataSourceSpecStorageSchema


class CHYTTableListCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CHYT_YTSAURUS_TABLE_LIST
    source_cls = CHYTTableListDataSource
    source_spec_cls = CHYTTableListDataSourceSpec
    us_storage_schema_cls = CHYTTableListDataSourceSpecStorageSchema


class CHYTTableRangeCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CHYT_YTSAURUS_TABLE_RANGE
    source_cls = CHYTTableRangeDataSource
    source_spec_cls = CHYTTableRangeDataSourceSpec
    us_storage_schema_cls = CHYTTableRangeDataSourceSpecStorageSchema


class CHYTTableSubselectCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CHYT_YTSAURUS_SUBSELECT
    source_cls = CHYTTableSubselectDataSource
    source_spec_cls = CHYTSubselectDataSourceSpec
    us_storage_schema_cls = CHYTSubselectDataSourceSpecStorageSchema


class CHYTCoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_CHYT
    connection_definitions = (CHYTCoreConnectionDefinition,)
    source_definitions = (
        CHYTTableCoreSourceDefinition,
        CHYTTableListCoreSourceDefinition,
        CHYTTableRangeCoreSourceDefinition,
        CHYTTableSubselectCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset(
        {
            CHYTAdapter,
            AsyncCHYTAdapter,
        }
    )
    query_cls = CHQuery
