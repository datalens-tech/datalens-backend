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
from dl_obfuscator import (
    ObfuscationContext,
    get_request_obfuscation_engine,
)


LOGGER = logging.getLogger(__name__)


def get_db_version_query(db_ident: DBIdent) -> DBAdapterQuery:
    """Return database version string query for this connection"""
    return DBAdapterQuery(sa.select([sa.func.version()]), db_name=db_ident.db_name)


def compile_query_for_debug(query: ClauseElement | str, dialect: Dialect) -> str:
    """
    Compile query to string.
    This function is only suitable for logging and not execution of the result.
    Its result might not be valid SQL if query contains date/datetime literals.
    """
    if isinstance(query, str):
        return query
    try:
        try:
            return str(query.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
        except NotImplementedError:
            compiled = query.compile(dialect=dialect)
            return make_debug_query(str(compiled), compiled.params)
    except Exception:
        LOGGER.exception("Failed to compile query for debug")
        return "-"


def compile_query_for_inspector(query: ClauseElement | str, dialect: Dialect) -> str:
    # TODO: BI-6448
    if isinstance(query, str):
        return query
    try:
        try:
            return str(query.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
        except NotImplementedError:
            compiled = query.compile(dialect=dialect)
            return make_debug_query(str(compiled), compiled.params)
    except Exception:
        LOGGER.exception("Failed to compile query for inspector")
        return "-"

    engine = get_request_obfuscation_engine()
    if engine is not None:
        compiled = engine.obfuscate(compiled, ObfuscationContext.INSPECTOR)

    return compiled


def compile_query_with_literal_binds_if_possible(
    query: ClauseElement | str,
    dialect: Dialect,
) -> ClauseElement | str:
    """
    Compile query to string using literal_binds.

    In case of error, defaults to query as-is without compilation
    """

    if isinstance(query, str):
        return query

    try:
        compiled_query = str(
            query.compile(
                dialect=dialect,
                compile_kwargs={"literal_binds": True},
            )
        )

        # SA generates query for DBAPI, so mod is represented as `%%` so next hack is needed
        return compiled_query % ()
    except Exception:
        LOGGER.exception("Failed to compile query with literal_binds")
        LOGGER.debug(f"Debug query: {compile_query_for_debug(query, dialect)}")
        return query


def make_debug_query(query: str, params: Union[list, dict]) -> str:
    return f"{query} {params!r}"


class CursorLogger:
    @staticmethod
    def before_cursor_execute_handler(conn, cursor, statement, parameters, context, executemany):  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation  [no-untyped-def]
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
    def after_cursor_execute_handler(conn, cursor, statement, parameters, context, executemany):  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation  [no-untyped-def]
        engine_url = context.engine.url
        query_start_time = conn.info["query_start_time"].pop(-1)

        span: opentracing.Span = conn.info["ot_span_scope_stack"].pop()
        span.finish()

        execution_time_seconds = time.monotonic() - query_start_time

        obfuscation_engine = get_request_obfuscation_engine()
        if obfuscation_engine is not None:
            statement = obfuscation_engine.obfuscate(statement, ObfuscationContext.TRACING)

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
