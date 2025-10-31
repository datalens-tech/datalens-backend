from __future__ import annotations

import contextlib
import typing
from typing import (
    Any,
    Callable,
    Optional,
)

import attr

from dl_core.connection_executors.adapters.adapters_base_sa_classic import BaseClassicAdapter
from dl_core.connection_executors.models.db_adapter_data import ExecutionStepCursorInfo
from dl_core.connection_models.common_models import (
    PageIdent,
    SchemaIdent,
    TableIdent,
)

from dl_connector_postgresql.core.postgresql_base.adapters_base_postgres import (
    OID_KNOWLEDGE,
    BasePostgresAdapter,
)
from dl_connector_postgresql.core.postgresql_base.error_transformer import sync_pg_db_error_transformer
from dl_connector_postgresql.core.postgresql_base.target_dto import PostgresConnTargetDTO


@attr.s()
class PostgresAdapter(BasePostgresAdapter, BaseClassicAdapter[PostgresConnTargetDTO]):
    _error_transformer = sync_pg_db_error_transformer

    execution_options = {
        "stream_results": True,
    }

    def get_connect_args(self) -> dict:
        return dict(
            super().get_connect_args(),
            sslmode="require" if self._target_dto.ssl_enable else "prefer",
            sslrootcert=self.get_ssl_cert_path(self._target_dto.ssl_ca),
        )

    def get_engine_kwargs(self) -> dict:
        result: dict = {}
        enforce_collate = self._get_enforce_collate(self._target_dto)
        if enforce_collate:
            result.update(enforce_collate=enforce_collate)
        return result

    @contextlib.contextmanager
    def execution_context(self) -> typing.Generator[None, None, None]:
        contexts: list[typing.ContextManager[None]] = [super().execution_context()]

        if self._target_dto.ssl_ca is not None:
            contexts.append(self.ssl_cert_context(self._target_dto.ssl_ca))

        with contextlib.ExitStack() as stack:
            for context in contexts:
                stack.enter_context(context)
            try:
                yield
            finally:
                stack.close()

    def _get_tables(self, schema_ident: SchemaIdent, page_ident: PageIdent | None = None) -> list[TableIdent]:
        db_name = schema_ident.db_name
        db_engine = self.get_db_engine(db_name)

        if not page_ident:
            page_ident = PageIdent()

        if schema_ident.schema_name is not None:
            table_and_view_query = self.get_list_table_and_view_names_query(
                schema_name=schema_ident.schema_name,
                search_text=page_ident.search_text,
                limit=page_ident.limit,
                offset=page_ident.offset,
            )
            result_rows = db_engine.execute(table_and_view_query, {"schema": schema_ident.schema_name})
            table_and_view_names = [row[0] for row in result_rows]
            result = ((schema_ident.schema_name, name) for name in table_and_view_names)
        else:
            assert schema_ident.schema_name is None
            all_tables_query = self.get_list_all_tables_query(
                search_text=page_ident.search_text, limit=page_ident.limit, offset=page_ident.offset
            )
            result = ((row[0], row[1]) for row in db_engine.execute(all_tables_query))

        return [
            TableIdent(
                db_name=db_name,
                schema_name=schema_name,
                table_name=name,
            )
            for schema_name, name in result
        ]

    def _make_cursor_info(self, cursor, db_session=None) -> dict:  # type: ignore  # TODO: fix
        return dict(
            super()._make_cursor_info(cursor, db_session=db_session),
            # Deprecating:
            columns=[
                dict(
                    name=str(column[0]),
                    postgresql_oid=column[1],
                    postgresql_typname=OID_KNOWLEDGE.get(column[1]),
                )
                for column in cursor.description
            ],
            # dashsql convenience:
            postgresql_typnames=[OID_KNOWLEDGE.get(column[1]) for column in cursor.description],
        )

    def _get_row_converters(self, cursor_info: ExecutionStepCursorInfo) -> tuple[Optional[Callable[[Any], Any]], ...]:
        return tuple(
            self._convert_bytea if col.type_code == 17 else None  # `bytea`
            for col in cursor_info.raw_cursor_description
        )
