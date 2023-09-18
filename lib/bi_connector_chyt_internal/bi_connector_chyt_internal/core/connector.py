from clickhouse_sqlalchemy.orm.query import Query as CHQuery

from dl_core.connectors.base.connector import (
    CoreConnector, CoreConnectionDefinition, CoreSourceDefinition,
)

from bi_connector_chyt_internal.core.constants import (
    BACKEND_TYPE_CHYT, CONNECTION_TYPE_CH_OVER_YT, CONNECTION_TYPE_CH_OVER_YT_USER_AUTH,
    SOURCE_TYPE_CHYT_TABLE, SOURCE_TYPE_CHYT_SUBSELECT,
    SOURCE_TYPE_CHYT_TABLE_LIST, SOURCE_TYPE_CHYT_TABLE_RANGE,
    SOURCE_TYPE_CHYT_USER_AUTH_TABLE, SOURCE_TYPE_CHYT_USER_AUTH_SUBSELECT,
    SOURCE_TYPE_CHYT_USER_AUTH_TABLE_LIST, SOURCE_TYPE_CHYT_USER_AUTH_TABLE_RANGE,
)
from bi_connector_chyt_internal.core.data_source import (
    CHYTInternalTableDataSource,
    CHYTInternalTableListDataSource,
    CHYTInternalTableRangeDataSource,
    CHYTInternalTableSubselectDataSource,
    CHYTUserAuthTableDataSource,
    CHYTUserAuthTableListDataSource,
    CHYTUserAuthTableRangeDataSource,
    CHYTUserAuthTableSubselectDataSource,
)
from bi_connector_chyt_internal.core.async_adapters import AsyncCHYTInternalAdapter, AsyncCHYTUserAuthAdapter
from dl_connector_chyt.core.data_source_spec import (
    CHYTTableDataSourceSpec,
    CHYTTableListDataSourceSpec,
    CHYTTableRangeDataSourceSpec,
    CHYTSubselectDataSourceSpec,
)
from dl_connector_chyt.core.storage_schemas.data_source_spec import (
    CHYTTableDataSourceSpecStorageSchema,
    CHYTTableListDataSourceSpecStorageSchema,
    CHYTTableRangeDataSourceSpecStorageSchema,
    CHYTSubselectDataSourceSpecStorageSchema,
)
from bi_connector_chyt_internal.core.us_connection import ConnectionCHYTInternalToken, ConnectionCHYTUserAuth
from bi_connector_chyt_internal.core.storage_schemas.connection import (
    ConnectionCHYTInternalTokenDataStorageSchema,
    ConnectionCHYTUserAuthDataStorageSchema,
)
from bi_connector_chyt_internal.core.type_transformer import CHYTUserAuthTypeTransformer, CHYTInternalTypeTransformer
from bi_connector_chyt_internal.core.connection_executors import (
    CHYTInternalSyncAdapterConnExecutor,
    CHYTUserAuthSyncAdapterConnExecutor,
    CHYTInternalAsyncAdapterConnExecutor,
    CHYTUserAuthAsyncAdapterConnExecutor,
)
from bi_connector_chyt_internal.core.adapters import CHYTInternalAdapter, CHYTUserAuthAdapter
from bi_connector_chyt_internal.core.sa_types import SQLALCHEMY_CHYT_INTERNAL_TYPES
from bi_connector_chyt_internal.core.settings import CHYTInternalSettingDefinition, CHYTUserAuthSettingDefinition
from bi_connector_chyt_internal.core.data_source_migration import (
    CHYTInternalDataSourceMigrator, CHYTInternalUserAuthDataSourceMigrator,
)
from bi_connector_chyt_internal.core.notifications import UsingPublicClickhouseCliqueNotification


class CHYTInternalCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_CH_OVER_YT
    connection_cls = ConnectionCHYTInternalToken
    us_storage_schema_cls = ConnectionCHYTInternalTokenDataStorageSchema
    type_transformer_cls = CHYTInternalTypeTransformer
    sync_conn_executor_cls = CHYTInternalSyncAdapterConnExecutor
    async_conn_executor_cls = CHYTInternalAsyncAdapterConnExecutor
    dialect_string = 'bi_chyt'
    settings_definition = CHYTInternalSettingDefinition
    data_source_migrator_cls = CHYTInternalDataSourceMigrator


class CHYTTableCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CHYT_TABLE
    source_cls = CHYTInternalTableDataSource
    source_spec_cls = CHYTTableDataSourceSpec
    us_storage_schema_cls = CHYTTableDataSourceSpecStorageSchema


class CHYTTableListCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CHYT_TABLE_LIST
    source_cls = CHYTInternalTableListDataSource
    source_spec_cls = CHYTTableListDataSourceSpec
    us_storage_schema_cls = CHYTTableListDataSourceSpecStorageSchema


class CHYTTableRangeCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CHYT_TABLE_RANGE
    source_cls = CHYTInternalTableRangeDataSource
    source_spec_cls = CHYTTableRangeDataSourceSpec
    us_storage_schema_cls = CHYTTableRangeDataSourceSpecStorageSchema


class CHYTTableSubselectCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CHYT_SUBSELECT
    source_cls = CHYTInternalTableSubselectDataSource
    source_spec_cls = CHYTSubselectDataSourceSpec
    us_storage_schema_cls = CHYTSubselectDataSourceSpecStorageSchema


class CHYTUserAuthCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_CH_OVER_YT_USER_AUTH
    connection_cls = ConnectionCHYTUserAuth
    us_storage_schema_cls = ConnectionCHYTUserAuthDataStorageSchema
    type_transformer_cls = CHYTUserAuthTypeTransformer
    sync_conn_executor_cls = CHYTUserAuthSyncAdapterConnExecutor
    async_conn_executor_cls = CHYTUserAuthAsyncAdapterConnExecutor
    dialect_string = 'bi_chyt'
    settings_definition = CHYTUserAuthSettingDefinition
    data_source_migrator_cls = CHYTInternalUserAuthDataSourceMigrator


class CHYTUserAuthTableCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CHYT_USER_AUTH_TABLE
    source_cls = CHYTUserAuthTableDataSource
    source_spec_cls = CHYTTableDataSourceSpec
    us_storage_schema_cls = CHYTTableDataSourceSpecStorageSchema


class CHYTUserAuthTableListCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CHYT_USER_AUTH_TABLE_LIST
    source_cls = CHYTUserAuthTableListDataSource
    source_spec_cls = CHYTTableListDataSourceSpec
    us_storage_schema_cls = CHYTTableListDataSourceSpecStorageSchema


class CHYTUserAuthTableRangeCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CHYT_USER_AUTH_TABLE_RANGE
    source_cls = CHYTUserAuthTableRangeDataSource
    source_spec_cls = CHYTTableRangeDataSourceSpec
    us_storage_schema_cls = CHYTTableRangeDataSourceSpecStorageSchema


class CHYTUserAuthTableSubselectCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_CHYT_USER_AUTH_SUBSELECT
    source_cls = CHYTUserAuthTableSubselectDataSource
    source_spec_cls = CHYTSubselectDataSourceSpec
    us_storage_schema_cls = CHYTSubselectDataSourceSpecStorageSchema


class CHYTInternalCoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_CHYT
    connection_definitions = (
        CHYTInternalCoreConnectionDefinition,
        CHYTUserAuthCoreConnectionDefinition,
    )
    source_definitions = (
        CHYTTableCoreSourceDefinition,
        CHYTTableListCoreSourceDefinition,
        CHYTTableRangeCoreSourceDefinition,
        CHYTTableSubselectCoreSourceDefinition,
        CHYTUserAuthTableCoreSourceDefinition,
        CHYTUserAuthTableListCoreSourceDefinition,
        CHYTUserAuthTableRangeCoreSourceDefinition,
        CHYTUserAuthTableSubselectCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({
        CHYTInternalAdapter,
        AsyncCHYTInternalAdapter,
        CHYTUserAuthAdapter,
        AsyncCHYTUserAuthAdapter,
    })
    sa_types = SQLALCHEMY_CHYT_INTERNAL_TYPES
    notification_classes = (
        UsingPublicClickhouseCliqueNotification,
    )
    query_cls = CHQuery
