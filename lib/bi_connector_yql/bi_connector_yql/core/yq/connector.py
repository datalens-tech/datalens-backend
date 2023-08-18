from bi_core.connectors.base.connector import (
    CoreConnectionDefinition, CoreConnector, CoreSourceDefinition,
)

from bi_connector_yql.core.yq.constants import (
    BACKEND_TYPE_YQ, CONNECTION_TYPE_YQ, SOURCE_TYPE_YQ_TABLE, SOURCE_TYPE_YQ_SUBSELECT
)
from bi_connector_yql.core.yq.adapter import YQAdapter
from bi_connector_yql.core.yq.connection_executors import YQAsyncAdapterConnExecutor
from bi_connector_yql.core.yq.data_source import YQSubselectDataSource, YQTableDataSource
from bi_connector_yql.core.yq.storage_schemas.connection import YQConnectionDataStorageSchema
from bi_connector_yql.core.yq.type_transformer import YQTypeTransformer
from bi_connector_yql.core.yq.us_connection import YQConnection
from bi_core.data_source_spec.sql import StandardSQLDataSourceSpec, SubselectDataSourceSpec
from bi_core.us_manager.storage_schemas.data_source_spec_base import (
    SQLDataSourceSpecStorageSchema, SubselectDataSourceSpecStorageSchema,
)


class YQCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_YQ
    connection_cls = YQConnection
    us_storage_schema_cls = YQConnectionDataStorageSchema
    type_transformer_cls = YQTypeTransformer
    sync_conn_executor_cls = YQAsyncAdapterConnExecutor
    async_conn_executor_cls = YQAsyncAdapterConnExecutor
    dialect_string = 'bi_yq'


class YQTableCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_YQ_TABLE
    source_cls = YQTableDataSource
    source_spec_cls = StandardSQLDataSourceSpec
    us_storage_schema_cls = SQLDataSourceSpecStorageSchema


class YQSubselectCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_YQ_SUBSELECT
    source_cls = YQSubselectDataSource
    source_spec_cls = SubselectDataSourceSpec
    us_storage_schema_cls = SubselectDataSourceSpecStorageSchema


class YQCoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_YQ
    connection_definitions = (
        YQCoreConnectionDefinition,
    )
    source_definitions = (
        YQTableCoreSourceDefinition,
        YQSubselectCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({YQAdapter})
