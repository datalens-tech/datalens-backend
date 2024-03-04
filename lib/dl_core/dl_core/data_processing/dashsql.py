from __future__ import annotations

import enum
import logging
import sys
import time
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    Mapping,
    Optional,
    Sequence,
)

import attr
import sqlalchemy as sa
from sqlalchemy.sql.elements import TextClause

from dl_api_commons.reporting.models import (
    QueryExecutionCacheInfoReportingRecord,
    QueryExecutionEndReportingRecord,
    QueryExecutionStartReportingRecord,
)
from dl_cache_engine.processing_helper import (
    CacheProcessingHelper,
    CacheSituation,
    TJSONExtChunkStream,
)
from dl_constants.types import TJSONExt  # not under `TYPE_CHECKING`, need to define new type aliases.
from dl_core import exc
from dl_core.backend_types import get_backend_type
from dl_core.base_models import WorkbookEntryLocation
from dl_core.connection_executors.common_base import ConnExecutorQuery
from dl_core.connectors.base.dashsql import get_custom_dash_sql_key_names
from dl_core.data_processing.cache.utils import DashSQLCacheOptionsBuilder
from dl_core.utils import (
    compile_query_for_debug,
    make_id,
    sa_plain_text,
)
from dl_dashsql.formatting.base import (
    FormattedQuery,
    QueryIncomingParameter,
)
from dl_dashsql.formatting.placeholder_dbapi import DBAPIQueryFormatterFactory
from dl_dashsql.registry import get_dash_sql_param_literalizer
from dl_dashsql.types import IncomingDSQLParamTypeExt
from dl_utils.streaming import (
    AsyncChunked,
    chunkify_by_one,
)


if TYPE_CHECKING:
    from dl_cache_engine.primitives import BIQueryCacheOptions
    from dl_core.connection_executors.async_base import (
        AsyncConnExecutorBase,
        AsyncExecutionResult,
    )
    from dl_core.services_registry import ServicesRegistry
    from dl_core.us_connection_base import ConnectionBase


LOGGER = logging.getLogger(__name__)


@enum.unique
class DashSQLEvent(enum.Enum):
    metadata = "metadata"
    row = "row"  # to be deprecated
    rowchunk = "rowchunk"
    error = "error"
    footer = "footer"


TRow = tuple[TJSONExt, ...]
TMeta = dict[str, TJSONExt]
# # More correct but mymy couldn't:
# TResultEvent = Union[
#     tuple[Literal['metadata'], TMeta],
#     tuple[Literal['row'], TRow],
#     tuple[Literal['rowchunk'], tuple[TRow, ...]],
#     tuple[Literal['error'], TMeta],
#     tuple[Literal['footer'], TMeta],
# ]
TResultEvent = tuple[str, TMeta | TRow]
TResultEvents = AsyncIterable[TResultEvent]


@attr.s(auto_attribs=True)
class DashSQLSelector:
    conn: ConnectionBase
    sql_query: str
    incoming_parameters: Optional[list[QueryIncomingParameter]]
    db_params: dict[str, str]
    _service_registry: ServicesRegistry
    connector_specific_params: Optional[Mapping[str, IncomingDSQLParamTypeExt]] = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        specific_param_keys = get_custom_dash_sql_key_names(conn_type=self.conn.conn_type)
        if specific_param_keys and self.connector_specific_params is None and self.incoming_parameters is not None:
            self.connector_specific_params = {
                param.original_name: param.value
                for param in self.incoming_parameters
                if param.original_name in specific_param_keys
            }
            self.incoming_parameters = [
                param for param in self.incoming_parameters if param.original_name not in specific_param_keys
            ]

    def make_ce(self) -> AsyncConnExecutorBase:
        ce_factory = self._service_registry.get_conn_executor_factory()
        ce = ce_factory.get_async_conn_executor(self.conn)
        ce = ce.mutate_for_dashsql(db_params=self.db_params)
        return ce

    def _formatted_query_to_sa_query(self, formatted_query: FormattedQuery) -> TextClause:
        backend_type = get_backend_type(conn_type=self.conn.conn_type)
        literalizer = get_dash_sql_param_literalizer(backend_type=backend_type)

        try:
            sa_text = sa.text(formatted_query.query).bindparams(
                *[
                    sa.bindparam(
                        param.param_name,
                        type_=literalizer.get_sa_type(
                            bi_type=param.incoming_param.user_type,
                            value_base=param.incoming_param.value,
                        ),
                        value=param.incoming_param.value,
                        expanding=isinstance(param.incoming_param.value, (list, tuple)),
                    )
                    for param in formatted_query.bound_params
                ]
            )
        except sa.exc.ArgumentError as e:
            raise exc.WrongQueryParameterization() from e

        return sa_text

    def _make_sa_query(self) -> tuple[TextClause, str]:
        query = self.sql_query
        if self.incoming_parameters is None:
            # No parameters substitution
            return sa_plain_text(query), query

        formatter_factory = DBAPIQueryFormatterFactory()  # TODO: Connectorize
        formatter = formatter_factory.get_query_formatter()
        formatting_result = formatter.format_query(query=query, incoming_parameters=self.incoming_parameters or [])
        formatted_query = formatting_result.formatted_query
        sa_query = self._formatted_query_to_sa_query(formatted_query)

        conn = self.conn
        dialect = conn.get_dialect()
        debug_text = compile_query_for_debug(self.sql_query, dialect=dialect)
        return sa_query, debug_text

    def make_ce_query(self) -> ConnExecutorQuery:
        sa_text, debug_compiled_query = self._make_sa_query()
        return ConnExecutorQuery(
            query=sa_text,  # type: ignore  # TODO: fixC
            # # TODO: try:
            # query=sql_query,
            debug_compiled_query=debug_compiled_query,
            connector_specific_params=self.connector_specific_params,
            autodetect_user_types=True,
            # connect_args=...,  # TODO: pass db_params to e.g. CH query arguments
            # db_name=None,
            # user_types=None,
            # chunk_size=None,
            is_dashsql_query=True,
        )

    @staticmethod
    async def event_gen(
        result_head: TMeta,
        result_chunks: AsyncIterable[Sequence[Any]],
        result_footer_holder: TMeta,
    ) -> TResultEvents:
        # Note: returning strings for easier jsonability.
        # Additionally, those strings are in the API output as-is.
        yield DashSQLEvent.metadata.value, result_head
        try:
            async for chunk in result_chunks:
                yield DashSQLEvent.rowchunk.value, tuple(chunk)
        except Exception as err:
            LOGGER.exception("Runtime error while fetching rows: %r", err)
            raise err
        else:
            yield DashSQLEvent.footer.value, result_footer_holder

    def process_result(self, exec_result: AsyncExecutionResult) -> TResultEvents:
        result_head = exec_result.cursor_info
        assert result_head
        db_types = result_head.get("db_types")
        user_types = exec_result.user_types
        result_head = dict(
            result_head,
            bi_types=[],
            db_types=[],
        )
        if user_types:
            result_head["bi_types"] = [bi_type.name if bi_type else None for bi_type in user_types]
        if db_types:
            result_head["db_types"] = [db_type.name if db_type else None for db_type in db_types]

        result_chunks = exec_result.result
        result_footer_holder: dict = {}  # mutable  # TODO: return footer from CE

        # Wrapping the rest in an additional function to have the code above
        # execute on call rather than on iteration.
        return self.event_gen(result_head, result_chunks, result_footer_holder)

    async def _execute(self, ce: AsyncConnExecutorBase, ce_query: ConnExecutorQuery) -> TResultEvents:
        """Simplified override point"""
        exec_result = await ce.execute(ce_query)
        return self.process_result(exec_result)

    async def execute(self) -> TResultEvents:
        if not self.conn.is_dashsql_allowed:
            raise exc.DashSQLNotAllowed()

        ce = self.make_ce()
        ce_query = self.make_ce_query()
        return await self._execute(ce, ce_query)


@attr.s
class DashSQLCachedSelector(DashSQLSelector):
    _is_bleeding_edge_user: bool = attr.ib(default=False)

    def make_query_id(self) -> str:
        return "dashsql_{}".format(make_id())

    def _get_jsonable_params(self) -> Optional[TJSONExt]:
        if self.incoming_parameters is None:
            return None
        return tuple(
            # `value` might contain e.g. datetimes
            (param.original_name, param.user_type.name, param.value)
            for param in self.incoming_parameters
        )

    def _get_jsonable_connector_specific_params(self) -> Optional[TJSONExt]:
        if self.connector_specific_params is None:
            return None
        return tuple((name, value) for name, value in self.connector_specific_params.items())

    def make_cache_options(self) -> BIQueryCacheOptions:
        cache_options_builder = DashSQLCacheOptionsBuilder(is_bleeding_edge_user=self._is_bleeding_edge_user)
        return cache_options_builder.get_cache_options(
            conn=self.conn,
            query_text=self.sql_query,
            params=self._get_jsonable_params(),
            db_params=self.db_params,
            connector_specific_params=self._get_jsonable_connector_specific_params(),
        )

    async def _execute(
        self,
        ce: AsyncConnExecutorBase,
        ce_query: ConnExecutorQuery,
    ) -> TResultEvents:
        async def _request_db() -> TResultEvents:  # basically just `super()`
            exec_result = await ce.execute(ce_query)
            return self.process_result(exec_result)

        async def _generate_func() -> Optional[TJSONExtChunkStream]:
            events = await _request_db()
            chunks = chunkify_by_one(events)  # not `await` for tricky reasons.
            # ^ At this point there's sometimes a chunk that contains one item
            # that is an Event with a chunk of rows in it.
            # TODO?: support type-annotated chunks in the `AsyncChunked`?
            return AsyncChunked(chunked_data=chunks)

        query_id = self.make_query_id()
        conn = self.conn
        conn_id = conn.uuid
        assert conn_id

        service_registry = self._service_registry
        workbook_id = service_registry.rci.workbook_id or (
            self.conn.entry_key.workbook_id if isinstance(self.conn.entry_key, WorkbookEntryLocation) else None
        )
        reporting_registry = service_registry.get_reporting_registry()

        reporting_registry.save_reporting_record(
            QueryExecutionStartReportingRecord(
                timestamp=time.time(),
                query_id=query_id,
                dataset_id=None,
                query_type=None,
                connection_type=conn.conn_type,
                conn_reporting_data=self.conn.get_conn_dto().conn_reporting_data(),
                query=str(ce_query.query),
                workbook_id=workbook_id,
            )
        )

        cache_engine_factory = service_registry.get_cache_engine_factory()
        assert cache_engine_factory is not None
        cache_engine = cache_engine_factory.get_cache_engine(entity_id=conn_id)
        cache_helper = CacheProcessingHelper(cache_engine=cache_engine)
        cache_options = self.make_cache_options()
        if not cache_options.cache_enabled:
            # cache not applicable
            try:
                return await _request_db()
            finally:
                _, exec_exception, _ = sys.exc_info()

                reporting_registry.save_reporting_record(
                    QueryExecutionEndReportingRecord(
                        timestamp=time.time(),
                        query_id=query_id,
                        exception=exec_exception,
                    )
                )

        # TODO: make this env-configurable through settings.
        use_locked_cache = self.conn.use_locked_cache

        cache_full_hit = None
        try:
            sit, result_stream = await cache_helper.run_with_cache(
                generate_func=_generate_func,
                cache_options=cache_options,
                use_locked_cache=use_locked_cache,
            )
            if sit == CacheSituation.full_hit:
                cache_full_hit = True
            elif sit == CacheSituation.generated:
                cache_full_hit = False
        finally:
            _, exec_exception, _ = sys.exc_info()

            reporting_registry.save_reporting_record(
                QueryExecutionCacheInfoReportingRecord(
                    query_id=query_id,
                    cache_full_hit=cache_full_hit,
                    timestamp=time.time(),
                )
            )

            reporting_registry.save_reporting_record(
                QueryExecutionEndReportingRecord(
                    timestamp=time.time(),
                    query_id=query_id,
                    exception=exec_exception,
                )
            )

        assert result_stream is not None
        return result_stream.items  # type: ignore  # TODO: fix
