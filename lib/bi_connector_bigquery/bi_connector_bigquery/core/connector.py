from bi_core.connectors.base.connector import (
    CoreConnector, CoreConnectionDefinition, CoreSourceDefinition,
)

from bi_connector_bigquery.core.constants import (
    BACKEND_TYPE_BIGQUERY, CONNECTION_TYPE_BIGQUERY,
    SOURCE_TYPE_BIGQUERY_TABLE, SOURCE_TYPE_BIGQUERY_SUBSELECT
)
from bi_connector_bigquery.core.query_compiler import BigQueryQueryCompiler
from bi_connector_bigquery.core.us_connection import ConnectionSQLBigQuery
from bi_connector_bigquery.core.type_transformer import BigQueryTypeTransformer
from bi_connector_bigquery.core.connection_executors import BigQueryAsyncConnExecutor
from bi_connector_bigquery.core.data_source_spec import (
    BigQueryTableDataSourceSpec, BigQuerySubselectDataSourceSpec,
)
from bi_connector_bigquery.core.data_source import (
    BigQueryTableDataSource, BigQuerySubselectDataSource,
)
from bi_connector_bigquery.core.adapters import BigQueryDefaultAdapter
from bi_connector_bigquery.core.storage_schemas.data_source_spec import (
    BigQueryTableDataSourceSpecStorageSchema,
    BigQuerySubselectDataSourceSpecStorageSchema,
)
from bi_connector_bigquery.core.storage_schemas.connection import BigQueryConnectionDataStorageSchema
from bi_connector_bigquery.core.sa_types import SQLALCHEMY_BIGQUERY_TYPES


class BigQueryCoreConnectionDefinition(CoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_BIGQUERY
    connection_cls = ConnectionSQLBigQuery
    us_storage_schema_cls = BigQueryConnectionDataStorageSchema
    type_transformer_cls = BigQueryTypeTransformer
    sync_conn_executor_cls = BigQueryAsyncConnExecutor
    async_conn_executor_cls = BigQueryAsyncConnExecutor
    dialect_string = 'bigquery'


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


class BigQueryCoreConnector(CoreConnector):
    backend_type = BACKEND_TYPE_BIGQUERY
    compiler_cls = BigQueryQueryCompiler
    connection_definitions = (
        BigQueryCoreConnectionDefinition,
    )
    source_definitions = (
        BigQueryCoreTableSourceDefinition,
        BigQueryCoreSubselectSourceDefinition,
    )
    rqe_adapter_classes = frozenset({BigQueryDefaultAdapter})
    sa_types = SQLALCHEMY_BIGQUERY_TYPES
