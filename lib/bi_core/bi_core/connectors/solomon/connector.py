from bi_constants.enums import ConnectionType

from bi_core.connectors.base.connector import (
    CoreConnectionDefinition,
    CoreConnector,
    CoreSourceDefinition,
)

from bi_core.connectors.solomon.constants import BACKEND_TYPE_SOLOMON, SOURCE_TYPE_SOLOMON
from bi_core.connectors.solomon.adapter import AsyncSolomonAdapter
from bi_core.connectors.solomon.storage_schemas.connection import ConnectionSolomonDataStorageSchema
from bi_core.connectors.solomon.type_transformer import SolomonTypeTransformer
from bi_core.connectors.solomon.us_connection import SolomonConnection
from bi_core.connectors.solomon.data_source import SolomonDataSource
from bi_core.connectors.solomon.connection_executors import SolomonAsyncAdapterConnExecutor


class SolomonCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = ConnectionType.solomon
    connection_cls = SolomonConnection
    us_storage_schema_cls = ConnectionSolomonDataStorageSchema
    type_transformer_cls = SolomonTypeTransformer
    sync_conn_executor_cls = SolomonAsyncAdapterConnExecutor
    async_conn_executor_cls = SolomonAsyncAdapterConnExecutor
    dialect_string = 'bi_solomon'


class SolomonCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_SOLOMON
    source_cls = SolomonDataSource


class SolomonCoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_SOLOMON
    connection_definitions = (
        SolomonCoreConnectionDefinition,
    )
    source_definitions = (
        SolomonCoreSourceDefinition,
    )
    rqe_adapter_classes = frozenset({AsyncSolomonAdapter})
