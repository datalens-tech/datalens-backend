from dl_core.connectors.base.connector import (
    CoreConnectionDefinition,
    CoreConnector,
    CoreSourceDefinition,
)

from bi_connector_solomon.core.adapter import AsyncSolomonAdapter
from bi_connector_solomon.core.connection_executors import SolomonAsyncAdapterConnExecutor
from bi_connector_solomon.core.constants import (
    BACKEND_TYPE_SOLOMON,
    CONNECTION_TYPE_SOLOMON,
    SOURCE_TYPE_SOLOMON,
)
from bi_connector_solomon.core.data_source import SolomonDataSource
from bi_connector_solomon.core.storage_schemas.connection import ConnectionSolomonDataStorageSchema
from bi_connector_solomon.core.type_transformer import SolomonTypeTransformer
from bi_connector_solomon.core.us_connection import SolomonConnection


class SolomonCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_SOLOMON
    connection_cls = SolomonConnection
    us_storage_schema_cls = ConnectionSolomonDataStorageSchema
    type_transformer_cls = SolomonTypeTransformer
    sync_conn_executor_cls = SolomonAsyncAdapterConnExecutor
    async_conn_executor_cls = SolomonAsyncAdapterConnExecutor
    dialect_string = "bi_solomon"
    custom_dashsql_key_names = frozenset(("from", "to", "project_id"))


class SolomonCoreSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_SOLOMON
    source_cls = SolomonDataSource


class SolomonCoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_SOLOMON
    connection_definitions = (SolomonCoreConnectionDefinition,)
    source_definitions = (SolomonCoreSourceDefinition,)
    rqe_adapter_classes = frozenset({AsyncSolomonAdapter})
