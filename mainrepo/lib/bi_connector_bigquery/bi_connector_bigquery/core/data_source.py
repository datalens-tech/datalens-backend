from __future__ import annotations

from typing import (
    Any,
    Optional,
)

from bi_constants.enums import CreateDSFrom
from bi_core.connection_models import (
    TableDefinition,
    TableIdent,
)
from bi_core.data_source.sql import (
    BaseSQLDataSource,
    SQLDataSource,
    SubselectDataSource,
    TableSQLDataSourceMixin,
    require_table_name,
)
from bi_core.utils import sa_plain_text

from bi_connector_bigquery.core.constants import (
    CONNECTION_TYPE_BIGQUERY,
    SOURCE_TYPE_BIGQUERY_SUBSELECT,
    SOURCE_TYPE_BIGQUERY_TABLE,
)
from bi_connector_bigquery.core.data_source_spec import (
    BigQuerySubselectDataSourceSpec,
    BigQueryTableDataSourceSpec,
)
from bi_connector_bigquery.core.query_compiler import BigQueryQueryCompiler
from bi_connector_bigquery.core.us_connection import ConnectionSQLBigQuery


class BigQueryDataSourceMixin(BaseSQLDataSource):
    conn_type = CONNECTION_TYPE_BIGQUERY

    @classmethod
    def is_compatible_with_type(cls, source_type: CreateDSFrom) -> bool:
        return source_type in (SOURCE_TYPE_BIGQUERY_TABLE, SOURCE_TYPE_BIGQUERY_SUBSELECT)


class BigQueryTableDataSource(BigQueryDataSourceMixin, TableSQLDataSourceMixin, SQLDataSource):
    """
    BigQuery table
    """

    compiler_cls = BigQueryQueryCompiler

    @property
    def spec(self) -> BigQueryTableDataSourceSpec:
        assert isinstance(self._spec, BigQueryTableDataSourceSpec)
        return self._spec

    @property
    def connection(self) -> ConnectionSQLBigQuery:
        connection = super().connection
        assert isinstance(connection, ConnectionSQLBigQuery)
        return connection

    @property
    def db_name(self) -> Optional[str]:
        return self.connection.project_id

    def get_table_definition(self) -> TableDefinition:
        assert self.table_name is not None
        return TableIdent(
            db_name=self.db_name,
            schema_name=self.spec.dataset_name,
            table_name=self.table_name,
        )

    def get_parameters(self) -> dict:
        return dict(
            super().get_parameters(),
            dataset_name=self.spec.dataset_name,
            table_name=self.spec.table_name,
        )

    @require_table_name
    def get_sql_source(self, alias: Optional[str] = None) -> Any:
        q = self.quote
        alias_str = "" if alias is None else f" AS {q(alias)}"
        return sa_plain_text(f"{q(self.db_name)}" f".{q(self.spec.dataset_name)}" f".{q(self.table_name)}{alias_str}")


class BigQuerySubselectDataSource(BigQueryDataSourceMixin, SubselectDataSource):
    """
    BigQuery subselect
    """

    @property
    def spec(self) -> BigQuerySubselectDataSourceSpec:
        assert isinstance(self._spec, BigQuerySubselectDataSourceSpec)
        return self._spec
