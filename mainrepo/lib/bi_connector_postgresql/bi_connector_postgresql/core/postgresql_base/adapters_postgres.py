from __future__ import annotations

import contextlib
import typing
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Tuple, Dict

import attr
import sqlalchemy as sa

from bi_core.connection_executors.adapters.adapters_base_sa_classic import BaseClassicAdapter
from bi_core.connection_models.common_models import TableIdent
from bi_connector_postgresql.core.postgresql_base.adapters_base_postgres import (
    OID_KNOWLEDGE,
    PG_LIST_SOURCES_ALL_SCHEMAS_SQL,
    BasePostgresAdapter,
)
from bi_connector_postgresql.core.postgresql_base.error_transformer import sync_pg_db_error_transformer
from bi_connector_postgresql.core.postgresql_base.target_dto import PostgresConnTargetDTO

if TYPE_CHECKING:
    from bi_core.connection_models.common_models import SchemaIdent
    from bi_core.connection_executors.models.db_adapter_data import ExecutionStepCursorInfo


@attr.s()
class PostgresAdapter(BasePostgresAdapter, BaseClassicAdapter[PostgresConnTargetDTO]):

    _error_transformer = sync_pg_db_error_transformer

    execution_options = {
        'stream_results': True,
    }

    def get_connect_args(self) -> dict:
        return {
            'sslmode': 'require' if self._target_dto.ssl_enable else 'prefer',
            'sslrootcert': self.get_ssl_cert_path(self._target_dto.ssl_ca),
        }

    def get_engine_kwargs(self) -> dict:
        result: Dict = {}
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

    def _get_tables(self, schema_ident: SchemaIdent) -> List[TableIdent]:
        if schema_ident.schema_name is not None:
            # For a single schema, plug into the common SA code.
            # (might not be ever used)
            return super()._get_tables(schema_ident)

        assert schema_ident.schema_name is None
        db_name = schema_ident.db_name
        db_engine = self.get_db_engine(db_name)
        query = PG_LIST_SOURCES_ALL_SCHEMAS_SQL
        result = db_engine.execute(sa.text(query))
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
                    postgresql_typname=OID_KNOWLEDGE.get(column[1]),  # type: ignore  # TODO: fix
                )
                for column in cursor.description],
            # dashsql convenience:
            postgresql_typnames=[OID_KNOWLEDGE.get(column[1]) for column in cursor.description],  # type: ignore  # TODO: fix
        )

    def _get_row_converters(self, cursor_info: ExecutionStepCursorInfo) -> Tuple[Optional[Callable[[Any], Any]], ...]:
        return tuple(
            self._convert_bytea if col.type_code == 17  # `bytea`
            else None
            for col in cursor_info.raw_cursor_description
        )
