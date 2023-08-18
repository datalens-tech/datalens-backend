from bi_connector_promql.core.storage_schemas.connection import PromQLConnectionDataStorageSchema
from bi_constants.enums import ConnectionType

from bi_core.connectors.base.connector import (
    CoreConnectionDefinition,
    CoreConnector,
    CoreSourceDefinition,
)

from bi_connector_promql.core.constants import BACKEND_TYPE_PROMQL, SOURCE_TYPE_PROMQL
from bi_connector_promql.core.adapter import AsyncPromQLAdapter, PromQLAdapter
from bi_connector_promql.core.connection_executors import (
    PromQLConnExecutor, PromQLAsyncAdapterConnExecutor,
)
from bi_connector_promql.core.type_transformer import PromQLTypeTransformer
from bi_connector_promql.core.us_connection import PromQLConnection
from bi_connector_promql.core.data_source import PromQLDataSource


class PromQLCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = ConnectionType.promql
    connection_cls = PromQLConnection
    us_storage_schema_cls = PromQLConnectionDataStorageSchema
    type_transformer_cls = PromQLTypeTransformer
    sync_conn_executor_cls = PromQLConnExecutor
    async_conn_executor_cls = PromQLAsyncAdapterConnExecutor
    dialect_string = 'bi_promql'


class PromQLCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_PROMQL
    source_cls = PromQLDataSource


class PromQLCoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_PROMQL
    connection_definitions = (
        PromQLCoreConnectionDefinition,
    )
    source_definitions = (
        PromQLCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({AsyncPromQLAdapter, PromQLAdapter})
