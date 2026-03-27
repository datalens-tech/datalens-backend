import re
from typing import Any

import attr
import sqlalchemy as sa

from dl_core.connection_executors.adapters.adapters_base_sa_classic import BaseClassicAdapter
from dl_core.connection_executors.models.db_adapter_data import (
    DBAdapterQuery,
    RawColumnInfo,
)
from dl_core.connection_models import (
    PageIdent,
    SchemaIdent,
    TableDefinition,
    TableIdent,
)
from dl_type_transformer.native_type import CommonNativeType

from dl_connector_starrocks.core.adapters_base_starrocks import BaseStarRocksAdapter
from dl_connector_starrocks.core.error_transformer import sync_starrocks_db_error_transformer
from dl_connector_starrocks.core.exc import StarRocksSourceDoesNotExistError
from dl_connector_starrocks.core.target_dto import StarRocksConnTargetDTO


STARROCKS_SYSTEM_SCHEMAS = ("information_schema", "_statistics_")

_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_\-]*$")


def _quote_ident(name: str) -> str:
    """Backtick-quote a StarRocks identifier, validating it first to prevent injection."""
    if not _SAFE_IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid StarRocks identifier: {name!r}")
    return f"`{name}`"


def _info_schema_ref(catalog: str, table: str) -> str:
    """Build a fully qualified reference: `catalog`.`information_schema`.`table`."""
    return f"{_quote_ident(catalog)}.`information_schema`.{_quote_ident(table)}"


def get_starrocks_tables_query(
    catalog: str,
    search_text: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> str:
    """Build a query to list tables from a StarRocks catalog.

    Uses fully qualified `catalog`.information_schema.tables syntax
    (supported since StarRocks v3.2 for both internal and external catalogs).
    Raw SQL is required because MySQL dialect backtick-quotes dotted schema names
    as a single identifier, which StarRocks rejects.
    """
    ref = _info_schema_ref(catalog, "tables")
    sql = (
        f"SELECT TABLE_SCHEMA, TABLE_NAME "
        f"FROM {ref} "
        f"WHERE TABLE_SCHEMA NOT IN ('information_schema', '_statistics_')"
    )

    if search_text is not None:
        # search_text is parameterized via SA text() in the caller
        sql += " AND TABLE_NAME LIKE :search_text"

    sql += " ORDER BY TABLE_SCHEMA, TABLE_NAME"

    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    if offset is not None:
        sql += f" OFFSET {int(offset)}"

    return sql


def get_starrocks_columns_query(catalog: str) -> str:
    """Build a query to get column info from a StarRocks catalog.
    Bind :database and :table_name when executing."""
    ref = _info_schema_ref(catalog, "columns")
    return (
        f"SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
        f"FROM {ref} "
        f"WHERE TABLE_SCHEMA = :database AND TABLE_NAME = :table_name "
        f"ORDER BY ORDINAL_POSITION"
    )


def get_starrocks_table_exists_query(catalog: str) -> str:
    """Build a query to check if a table exists in a StarRocks catalog.
    Bind :database and :table_name when executing."""
    ref = _info_schema_ref(catalog, "tables")
    return (
        f"SELECT COUNT(*) "
        f"FROM {ref} "
        f"WHERE TABLE_SCHEMA = :database AND TABLE_NAME = :table_name"
    )


@attr.s()
class StarRocksAdapter(BaseStarRocksAdapter, BaseClassicAdapter[StarRocksConnTargetDTO]):
    _target_dto: StarRocksConnTargetDTO = attr.ib()

    execution_options = {
        "stream_results": True,
    }

    _error_transformer = sync_starrocks_db_error_transformer

    def get_connect_args(self) -> dict:
        return dict(
            super().get_connect_args(),
            charset="utf8mb4",
            local_infile=0,
        )

    def get_default_db_name(self) -> str:
        return ""  # StarRocks doesn't require a catalog to connect

    def get_conn_line(self, db_name: str | None = None, params: dict[str, Any] | None = None) -> str:
        # StarRocks MySQL protocol doesn't support catalogs in the connection URL.
        # Metadata is queried via fully qualified `catalog`.information_schema.<table>.
        params = params or {}
        return (
            f"dl_mysql://"
            f"{self._target_dto.username}:{self._target_dto.password}"
            f"@{self._target_dto.host}:{self._target_dto.port}/"
        )

    def _get_tables(self, schema_ident: SchemaIdent, page_ident: PageIdent | None = None) -> list[TableIdent]:
        catalog = schema_ident.db_name or ""
        if not catalog:
            return []

        if page_ident:
            sql = get_starrocks_tables_query(
                catalog=catalog,
                search_text=page_ident.search_text,
                limit=page_ident.limit,
                offset=page_ident.offset,
            )
            bind_params: dict[str, Any] = {}
            if page_ident.search_text is not None:
                bind_params["search_text"] = f"%{page_ident.search_text}%"
            query = sa.text(sql).bindparams(**bind_params) if bind_params else sql
        else:
            query = get_starrocks_tables_query(catalog=catalog)

        result = self.execute(DBAdapterQuery(query))
        return [
            TableIdent(
                db_name=catalog,
                schema_name=schema_name,
                table_name=table_name,
            )
            for schema_name, table_name in result.get_all()
        ]

    def _get_raw_columns_info(self, table_def: TableDefinition) -> tuple[RawColumnInfo, ...]:
        if isinstance(table_def, TableIdent):
            table_ident = table_def
        else:
            table_ident = table_def.table_ident  # type: ignore

        catalog = table_ident.db_name
        database = table_ident.schema_name
        if not catalog or not database:
            raise ValueError("Both catalog (db_name) and database (schema_name) are required")

        sql = get_starrocks_columns_query(catalog=catalog)
        query = sa.text(sql).bindparams(database=database, table_name=table_ident.table_name)
        result = self.execute(DBAdapterQuery(query))
        rows = result.get_all()

        if not rows:
            raise StarRocksSourceDoesNotExistError(
                db_message=f"Table '{catalog}.{database}.{table_ident.table_name}' doesn't exist",
            )

        return tuple(
            RawColumnInfo(
                name=str(column_name),
                title=None,
                nullable=is_nullable == "YES",
                native_type=CommonNativeType.normalize_name_and_create(
                    name=str(data_type),
                    nullable=is_nullable == "YES",
                ),
            )
            for column_name, data_type, is_nullable in rows
        )

    def _is_table_exists(self, table_ident: TableIdent) -> bool:
        catalog = table_ident.db_name
        database = table_ident.schema_name
        if not catalog or not database:
            raise ValueError("Both catalog (db_name) and database (schema_name) are required")

        sql = get_starrocks_table_exists_query(catalog=catalog)
        query = sa.text(sql).bindparams(database=database, table_name=table_ident.table_name)
        result = self.execute(DBAdapterQuery(query))
        return result.get_all()[0][0] > 0
