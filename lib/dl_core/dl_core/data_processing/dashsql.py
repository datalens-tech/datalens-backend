from collections.abc import Mapping
import logging
import sys
import time

import attr
import sqlalchemy as sa
from sqlalchemy.sql.elements import TextClause

from dl_api_commons.reporting.models import (
    QueryExecutionCacheInfoReportingRecord,
    QueryExecutionEndReportingRecord,
    QueryExecutionStartReportingRecord,
)
from dl_cache_engine.primitives import BIQueryCacheOptions
from dl_cache_engine.processing_helper import (
    CacheProcessingHelper,
    CacheSituation,
    TJSONExtChunkStream,
)
from dl_constants.types import TJSONExt
from dl_core import exc
from dl_core.backend_types import get_backend_type
from dl_core.base_models import WorkbookEntryLocation
from dl_core.connection_executors.adapters.sa_utils import (
    compile_query_for_debug,
    compile_query_for_inspector,
)
from dl_core.connection_executors.async_base import AsyncConnExecutorBase
from dl_core.connection_executors.common_base import ConnExecutorQuery
from dl_core.connectors.base.dashsql import get_custom_dash_sql_key_names
from dl_core.data_processing.cache.utils import DashSQLCacheOptionsBuilder
from dl_core.data_processing.sql_selector_base import (
    BaseSQLSelector,
    SQLSelectorEvent,
    TResultEvents,
)
from dl_core.utils import (
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

LOGGER = logging.getLogger(__name__)


DashSQLEvent = SQLSelectorEvent


@attr.s(auto_attribs=True)
class DashSQLSelector(BaseSQLSelector):
    sql_query: str
    incoming_parameters: list[QueryIncomingParameter] | None
    db_params: dict[str, str]
    connector_specific_params: Mapping[str, IncomingDSQLParamTypeExt] | None = attr.ib(default=None)

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

    def _mutate_ce(self, ce: AsyncConnExecutorBase) -> AsyncConnExecutorBase:
        return ce.mutate_for_dashsql(db_params=self.db_params)

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
            raise exc.WrongQueryParameterizationError() from e

        return sa_text

    def _make_sa_query(self) -> tuple[TextClause, str, str]:
        query = self.sql_query
        if self.incoming_parameters is None:
            # No parameters substitution
            return sa_plain_text(query), query, query

        formatter_factory = DBAPIQueryFormatterFactory()  # TODO: Connectorize
        formatter = formatter_factory.get_query_formatter()
        formatting_result = formatter.format_query(query=query, incoming_parameters=self.incoming_parameters or [])
        formatted_query = formatting_result.formatted_query
        sa_query = self._formatted_query_to_sa_query(formatted_query)

        conn = self.conn
        dialect = conn.get_dialect()
        debug_text = compile_query_for_debug(self.sql_query, dialect=dialect)
        obfuscation_engine = self._service_registry.rci.obfuscation_engine
        inspector_text = compile_query_for_inspector(
            self.sql_query, dialect=dialect, obfuscation_engine=obfuscation_engine
        )
        return sa_query, debug_text, inspector_text

    def make_ce_query(self) -> ConnExecutorQuery:
        sa_text, debug_query, inspector_query = self._make_sa_query()
        return ConnExecutorQuery(
            query=sa_text,
            # # TODO: try:
            # query=sql_query,
            debug_compiled_query=debug_query,
            inspector_query=inspector_query,
            connector_specific_params=self.connector_specific_params,
            autodetect_user_types=True,
            # connect_args=...,  # TODO: pass db_params to e.g. CH query arguments
            # db_name=None,
            # user_types=None,
            # chunk_size=None,
            is_dashsql_query=True,
        )

    async def execute(self) -> TResultEvents:
        if not self.conn.is_dashsql_allowed:
            raise exc.DashSQLNotAllowedError()
        return await super().execute()


@attr.s
class DashSQLCachedSelector(DashSQLSelector):
    _is_bleeding_edge_user: bool = attr.ib(default=False)

    def make_query_id(self) -> str:
        return f"dashsql_{make_id()}"

    def _get_jsonable_params(self) -> TJSONExt | None:
        if self.incoming_parameters is None:
            return None
        return tuple(
            # `value` might contain e.g. datetimes
            (param.original_name, param.user_type.name, param.value)
            for param in self.incoming_parameters
        )

    def _get_jsonable_connector_specific_params(self) -> TJSONExt | None:
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

        async def _generate_func() -> TJSONExtChunkStream | None:
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
