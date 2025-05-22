from dl_core.connectors.base.connector import (
    CoreBackendDefinition,
    CoreConnectionDefinition,
    CoreConnector,
    CoreSourceDefinition,
)

from dl_connector_bigquery.core.adapters import BigQueryDefaultAdapter
from dl_connector_bigquery.core.connection_executors import BigQueryAsyncConnExecutor
from dl_connector_bigquery.core.constants import (
    BACKEND_TYPE_BIGQUERY,
    CONNECTION_TYPE_BIGQUERY,
    SOURCE_TYPE_BIGQUERY_SUBSELECT,
    SOURCE_TYPE_BIGQUERY_TABLE,
)
from dl_connector_bigquery.core.data_source import (
    BigQuerySubselectDataSource,
    BigQueryTableDataSource,
)
from dl_connector_bigquery.core.data_source_spec import (
    BigQuerySubselectDataSourceSpec,
    BigQueryTableDataSourceSpec,
)
from dl_connector_bigquery.core.query_compiler import BigQueryQueryCompiler
from dl_connector_bigquery.core.sa_types import SQLALCHEMY_BIGQUERY_TYPES
from dl_connector_bigquery.core.storage_schemas.connection import BigQueryConnectionDataStorageSchema
from dl_connector_bigquery.core.storage_schemas.data_source_spec import (
    BigQuerySubselectDataSourceSpecStorageSchema,
    BigQueryTableDataSourceSpecStorageSchema,
)
from dl_connector_bigquery.core.type_transformer import BigQueryTypeTransformer
from dl_connector_bigquery.core.us_connection import ConnectionSQLBigQuery


class BigQueryCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_BIGQUERY
    connection_cls = ConnectionSQLBigQuery
    us_storage_schema_cls = BigQueryConnectionDataStorageSchema
    type_transformer_cls = BigQueryTypeTransformer
    sync_conn_executor_cls = BigQueryAsyncConnExecutor
    async_conn_executor_cls = BigQueryAsyncConnExecutor
    dialect_string = "bigquery"
    allow_export = True


class BigQueryCoreTableSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_BIGQUERY_TABLE
    source_cls = BigQueryTableDataSource
    source_spec_cls = BigQueryTableDataSourceSpec
    us_storage_schema_cls = BigQueryTableDataSourceSpecStorageSchema


class BigQueryCoreSubselectSourceDefinition(CoreSourceDefinition):
    source_type = SOURCE_TYPE_BIGQUERY_SUBSELECT
    source_cls = BigQuerySubselectDataSource
    source_spec_cls = BigQuerySubselectDataSourceSpec
    us_storage_schema_cls = BigQuerySubselectDataSourceSpecStorageSchema


class BigQueryCoreBackendDefinition(CoreBackendDefinition):
    backend_type = BACKEND_TYPE_BIGQUERY
    compiler_cls = BigQueryQueryCompiler


class BigQueryCoreConnector(CoreConnector):
    backend_definition = BigQueryCoreBackendDefinition
    connection_definitions = (BigQueryCoreConnectionDefinition,)
    source_definitions = (
        BigQueryCoreTableSourceDefinition,
        BigQueryCoreSubselectSourceDefinition,
    )
    rqe_adapter_classes = frozenset({BigQueryDefaultAdapter})
    sa_types = SQLALCHEMY_BIGQUERY_TYPES
