from __future__ import annotations

import logging
import time
from typing import Union

import opentracing
import sqlalchemy as sa
from sqlalchemy.engine import Dialect
from sqlalchemy.sql.elements import ClauseElement

from dl_core.connection_executors.models.db_adapter_data import DBAdapterQuery
from dl_core.connection_models import DBIdent


LOGGER = logging.getLogger(__name__)


def get_db_version_query(db_ident: DBIdent) -> DBAdapterQuery:
    """Return database version string query for this connection"""
    return DBAdapterQuery(sa.select([sa.func.version()]), db_name=db_ident.db_name)


def compile_query_for_debug(query: ClauseElement, dialect: Dialect) -> str:
    """
    Compile query to string.
    This function is only suitable for logging and not execution of the result.
    Its result might not be valid SQL if query contains date/datetime literals.
    """
    # noinspection PyBroadException
    try:
        try:
            return str(query.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
        except NotImplementedError:
            compiled = query.compile(dialect=dialect)
            return make_debug_query(str(query), compiled.params)
    except Exception:
        return "-"


def make_debug_query(query: str, params: Union[list, dict]) -> str:
    return f"{query} {params!r}"


class CursorLogger:
    @staticmethod
    def before_cursor_execute_handler(conn, cursor, statement, parameters, context, executemany):
        tracer = opentracing.global_tracer()
        # Scope was not created due to decoupled span closing procedure
        #  which may cause broken parent-child relationships in case when `after_cursor_execute_handler()`
        #  will not be called
        span = tracer.start_span("sa-cursor-execute")

        engine_url = context.engine.url
        span.log_kv(
            dict(
                sa_dialect=engine_url.drivername,
            )
        )

        conn.info.setdefault("query_start_time", []).append(time.monotonic())
        conn.info.setdefault("ot_span_scope_stack", []).append(span)

    @staticmethod
    def after_cursor_execute_handler(conn, cursor, statement, parameters, context, executemany):
        engine_url = context.engine.url
        query_start_time = conn.info["query_start_time"].pop(-1)

        span: opentracing.Span = conn.info["ot_span_scope_stack"].pop()
        span.finish()

        execution_time_seconds = time.monotonic() - query_start_time

        extra = dict(
            event_code="db_exec",
            username=engine_url.username,
            query_id=str(getattr(cursor, "_query_id", None)),
            # missing: folder_id
            execution_time=int(round(execution_time_seconds * 1000)),
            database=engine_url.database,
            host=engine_url.host,
            drivername=engine_url.drivername,
            statement=statement,
            parameters_size=len(parameters),
            # missing: dataset_id
            # missing: connection_id
        )
        LOGGER.info("Cursor executed", extra=extra)
