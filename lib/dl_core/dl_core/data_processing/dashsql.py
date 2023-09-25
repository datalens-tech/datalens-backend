from __future__ import annotations

import enum
import logging
import re
import sys
import time
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import attr
import sqlalchemy as sa

from dl_api_commons.reporting.models import (
    QueryExecutionCacheInfoReportingRecord,
    QueryExecutionEndReportingRecord,
    QueryExecutionStartReportingRecord,
)
from dl_constants.types import TJSONExt  # not under `TYPE_CHECKING`, need to define new type aliases.
from dl_core import exc
from dl_core.connection_executors.common_base import ConnExecutorQuery
from dl_core.connectors.base.dashsql import get_custom_dash_sql_key_names
from dl_core.data_processing.cache.processing_helper import (
    CacheProcessingHelper,
    CacheSituation,
)
from dl_core.data_processing.cache.utils import DashSQLCacheOptionsBuilder
from dl_core.data_processing.streaming import (
    AsyncChunked,
    chunkify_by_one,
)
from dl_core.us_connection_base import ExecutorBasedMixin
from dl_core.utils import (
    compile_query_for_debug,
    make_id,
    sa_plain_text,
)


if TYPE_CHECKING:
    from dl_core.connection_executors.async_base import (
        AsyncConnExecutorBase,
        AsyncExecutionResult,
    )
    from dl_core.data_processing.cache.primitives import BIQueryCacheOptions
    from dl_core.data_processing.types import TJSONExtChunkStream
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


TRow = Tuple[TJSONExt, ...]
TMeta = Dict[str, TJSONExt]
# # More correct but mymy couldn't:
# TResultEvent = Union[
#     Tuple[Literal['metadata'], TMeta],
#     Tuple[Literal['row'], TRow],
#     Tuple[Literal['rowchunk'], Tuple[TRow, ...]],
#     Tuple[Literal['error'], TMeta],
#     Tuple[Literal['footer'], TMeta],
# ]
TResultEvent = Tuple[str, Union[TMeta, TRow]]
TResultEvents = AsyncIterable[TResultEvent]


@attr.s(frozen=True, auto_attribs=True)
class BindParamInfo:
    name: str
    type_name: str
    sa_type: sa.sql.type_api.TypeEngine
    value: Any
    expanding: bool = False


@attr.s
class ParamsReformatter:
    query: str = attr.ib()
    existing_params: Set[str] = attr.ib()

    found_params: Set[str] = attr.ib(factory=set)  # for optionally skipping bindparams
    known_params: Set[str] = attr.ib(factory=set)  # for the uniqueness checks
    params_namemap: Dict[str, str] = attr.ib(factory=dict)
    pcounter: int = attr.ib(default=1)  # for making new unique names
    nameprefix: str = attr.ib(default="p")

    def __attrs_post_init__(self) -> None:
        self.known_params.update(self.existing_params)

    def prenormalize_param_name(self, name: str) -> str:
        """To make sure there're no catches with `:param`, normalize param names"""
        name = re.sub(r"[^A-Za-z0-9]", "_", name)
        if not re.search(r"^[A-Za-z]", name):
            name = f"{self.nameprefix}{name}"
        return name

    def normalize_param_name(self, name: str) -> str:
        mapped = self.params_namemap.get(name)
        if mapped is not None:
            return mapped
        mapped = self.prenormalize_param_name(name)
        if mapped == name:
            return name

        if mapped in self.known_params:
            while True:
                candidate = f"{mapped}_{self.pcounter}"
                if candidate not in self.known_params:
                    mapped = candidate
                    break
                self.pcounter += 1

        self.params_namemap[name] = mapped
        self.known_params.add(mapped)
        return mapped

    def param_repl(self, match: re.Match) -> str:
        param_name = match.group(1)
        if param_name not in self.existing_params:
            return match.group(0)  # hacky: leave as-is

        self.found_params.add(param_name)
        mapped_name = self.normalize_param_name(param_name)
        return f":{mapped_name}"

    def reformat(self) -> str:
        """Convert from `{{paramname}}` to `:paramname`"""
        query = self.query
        query = query.replace(":", r"\:")
        query = re.sub(r"\{\{([^}]+)\}\}", self.param_repl, query)
        return query


@attr.s(auto_attribs=True)
class DashSQLSelector:
    conn: ConnectionBase
    sql_query: str
    params: Optional[List[BindParamInfo]]
    db_params: Dict[str, str]
    _service_registry: ServicesRegistry
    connector_specific_params: Optional[Dict[str, TJSONExt]] = attr.ib(default=None)

    def __attrs_post_init__(self) -> None:
        specific_param_keys = get_custom_dash_sql_key_names(conn_type=self.conn.conn_type)
        if specific_param_keys and self.connector_specific_params is None and self.params is not None:
            self.connector_specific_params = {
                param.name: param.value for param in self.params if param.name in specific_param_keys
            }
            self.params = [param for param in self.params if param.name not in specific_param_keys]

    def make_ce(self) -> AsyncConnExecutorBase:
        ce_factory = self._service_registry.get_conn_executor_factory()
        ce = ce_factory.get_async_conn_executor(self.conn)
        ce = ce.mutate_for_dashsql(db_params=self.db_params)
        return ce

    def _make_sa_query(self) -> Tuple[sa.sql.elements.TextClause, str]:
        query = self.sql_query
        params = self.params
        if params is None:
            # No parameters substitution
            return sa_plain_text(query), query

        reformatter = ParamsReformatter(
            query=query,
            existing_params={param.name for param in params},
        )
        query = reformatter.reformat()
        try:
            sa_text = sa.text(query).bindparams(
                *[
                    sa.bindparam(
                        reformatter.normalize_param_name(param.name),
                        type_=param.sa_type,
                        value=param.value,
                        expanding=param.expanding,
                    )
                    for param in params
                    # dubious current behavior: skip unknown params
                    if param.name in reformatter.found_params
                ]
            )
        except sa.exc.ArgumentError:
            raise exc.WrongQueryParameterization()
        conn = self.conn
        # This should've been `dialect = conn.get_dialect() if isinstance(conn, ExecutorBasedMixin) else None`,
        # but mypy couldn't handle that.
        if isinstance(conn, ExecutorBasedMixin):
            dialect = conn.get_dialect()
        else:
            dialect = None  # type: ignore  # TODO: fix
        debug_text = compile_query_for_debug(self.sql_query, dialect=dialect)
        return sa_text, debug_text

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
        if self.params is None:
            return None
        return tuple(
            # `value` might contain e.g. datetimes
            (param.name, param.type_name, param.value, param.expanding)
            for param in self.params
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
        assert isinstance(self.conn, ExecutorBasedMixin), "connection must be derived from ExecutorBasedMixin"
        conn = self.conn
        conn_id = conn.uuid
        assert conn_id

        service_registry = self._service_registry
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
            )
        )

        cache_helper = CacheProcessingHelper(
            entity_id=conn_id,
            service_registry=service_registry,
        )
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
