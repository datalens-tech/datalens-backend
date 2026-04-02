from urllib.parse import quote

import attr

from dl_core.connection_executors.adapters.adapters_base_sa_classic import BaseClassicAdapter
from dl_core.connection_executors.adapters.common_base import get_dialect_string
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
import dl_core.exc as exc
from dl_type_transformer.native_type import CommonNativeType

from dl_connector_starrocks.core.adapters_base_starrocks import BaseStarRocksAdapter
from dl_connector_starrocks.core.error_transformer import sync_starrocks_db_error_transformer
from dl_connector_starrocks.core.target_dto import StarRocksConnTargetDTO


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

    def get_conn_line(self, db_name: str | None = None, params: dict | None = None) -> str:
        # StarRocksConnTargetDTO has no db_name (StarRocks connects without a default database),
        # so we can't use the base ClassicSQLConnLineConstructor which expects BaseSQLConnTargetDTO.
        dialect = get_dialect_string(self.conn_type)
        user = quote(self._target_dto.username, safe="")
        passwd = quote(self._target_dto.password, safe="")
        host = quote(self._target_dto.host, safe="")
        port = self._target_dto.port
        return f"{dialect}://{user}:{passwd}@{host}:{port}/"

    def _get_tables(self, schema_ident: SchemaIdent, page_ident: PageIdent | None = None) -> list[TableIdent]:
        catalog = schema_ident.db_name or ""
        if not catalog:
            return []

        query = self.get_list_tables_query(
            catalog=catalog,
            search_text=page_ident.search_text if page_ident else None,
            limit=page_ident.limit if page_ident else None,
            offset=page_ident.offset if page_ident else None,
        )

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
        if not isinstance(table_def, TableIdent):
            raise NotImplementedError(f"Unsupported table definition type: {type(table_def)}")

        table_ident = table_def
        catalog = table_ident.db_name
        database = table_ident.schema_name
        if not catalog or not database:
            raise ValueError("Both catalog (db_name) and database (schema_name) are required")

        query = self.get_table_info_query(catalog=catalog, database=database, table_name=table_ident.table_name)
        result = self.execute(DBAdapterQuery(query))
        rows = result.get_all()

        if not rows:
            raise exc.SourceDoesNotExist(
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

        query = self.get_table_exists_query(catalog=catalog, database=database, table_name=table_ident.table_name)
        result = self.execute(DBAdapterQuery(query))
        return result.get_all()[0][0] > 0
