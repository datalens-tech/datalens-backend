from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from aiohttp import web

from dl_api_lib.app.data_api.resources.base import (
    RequiredResourceDSAPI,
    requires,
)
from dl_api_lib.app.data_api.resources.dataset.base import DatasetDataBaseView
from dl_api_lib.enums import USPermissionKind
from dl_api_lib.exc import (
    CacheInvalidationTestError,
    CacheInvalidationTestInvalidResultError,
    CacheInvalidationTestModeOffError,
    CacheInvalidationTestNonStringResultError,
    CacheInvalidationTestNotEditorError,
    CacheInvalidationTestQueryError,
    CacheInvalidationTestSubselectNotAllowedError,
)
from dl_api_lib.query.formalization.raw_specs import (
    IdFieldRef,
    RawFilterFieldSpec,
    RawQueryMetaInfo,
    RawQuerySpecUnion,
    RawSelectFieldSpec,
)
import dl_api_lib.schemas.cache_invalidation_test as cache_invalidation_test_schemas
from dl_api_lib.utils.base import check_permission_on_entry
from dl_app_tools.profiling_base import generic_profiler_async
from dl_constants.enums import (
    CacheInvalidationMode,
    UserDataType,
)
from dl_core.components.accessor import DatasetComponentAccessor
from dl_core.connection_executors.common_base import ConnExecutorQuery
from dl_core.data_source.collection import DataSourceCollectionFactory
from dl_core.fields import ResultSchema
from dl_core.us_connection_base import (
    ConnectionBase,
    RawSqlLevelConnectionMixin,
)
from dl_core.utils import sa_plain_text
from dl_query_processing.enums import (
    GroupByPolicy,
    QueryType,
)


if TYPE_CHECKING:
    from aiohttp.web_response import Response

    from dl_core.cache_invalidation import CacheInvalidationSource


LOGGER = logging.getLogger(__name__)

_MAX_VALUE_LENGTH = 100


class DatasetCacheInvalidationTestView(DatasetDataBaseView):
    """
    Endpoint for testing cache invalidation queries.

    This request does not affect the main cache and does not take into account
    the configured invalidation cache throttling. It is intended for debugging
    the invalidation query by the user.

    Returns 400 when:
    - The user is not an editor of the dataset (risk of exposing RLS-restricted data).
    - The connection does not support subqueries (sql mode).
    - The query execution fails.
    - The result type is not string.
    - The result contains more than one row or the value exceeds 100 characters.
    """

    endpoint_code = "DatasetCacheInvalidationTest"
    profiler_prefix = "cache-invalidation-test"

    STORED_DATASET_REQUIRED = True

    @property
    def allow_query_cache_usage(self) -> bool:
        """Always bypass query cache for cache invalidation test — we need fresh data from the database."""
        return False

    @generic_profiler_async("ds-cache-invalidation-test-full")
    @DatasetDataBaseView.with_dataset_us_context
    @DatasetDataBaseView.with_resolved_entities
    @requires(RequiredResourceDSAPI.JSON_REQUEST)
    async def post(self) -> Response:
        schema = cache_invalidation_test_schemas.CacheInvalidationTestRequestSchema()
        req_model = schema.load(self.dl_request.json)

        if not check_permission_on_entry(self.dataset, USPermissionKind.edit):
            raise CacheInvalidationTestNotEditorError()

        await self.prepare_dataset_for_request(req_model=req_model)

        cache_invalidation_source = self.dataset.data.cache_invalidation_source

        if cache_invalidation_source.mode == CacheInvalidationMode.off:
            raise CacheInvalidationTestModeOffError()

        if cache_invalidation_source.mode == CacheInvalidationMode.sql:
            sql_query = cache_invalidation_source.sql or ""
            if not sql_query.strip():
                raise CacheInvalidationTestQueryError("Empty cache invalidation SQL query is not allowed")
            return await self._execute_sql_mode(sql_query=sql_query)

        if cache_invalidation_source.mode == CacheInvalidationMode.formula:
            return await self._execute_formula_mode(cache_invalidation_source=cache_invalidation_source)

        raise CacheInvalidationTestError(
            message=f"Unsupported cache invalidation mode: {cache_invalidation_source.mode}"
        )

    @staticmethod
    def _validate_single_row(row_count: int) -> None:
        if row_count == 0:
            raise CacheInvalidationTestInvalidResultError(
                message="Query returned no rows",
            )
        if row_count > 1:
            raise CacheInvalidationTestInvalidResultError(
                message=f"Expected exactly 1 row, got {row_count}",
            )

    @staticmethod
    def _validate_single_column(row: list | tuple) -> None:
        if len(row) == 0:
            raise CacheInvalidationTestInvalidResultError(
                message="Result row has no columns",
            )
        if len(row) > 1:
            raise CacheInvalidationTestInvalidResultError(
                message=f"Expected exactly 1 column, got {len(row)}",
            )

    @staticmethod
    def _validate_string_value(value: object) -> str:
        if not isinstance(value, str):
            raise CacheInvalidationTestNonStringResultError(
                message=f"Expected string result, got {type(value).__name__}",
            )

        value_str = str(value)
        if len(value_str) > _MAX_VALUE_LENGTH:
            raise CacheInvalidationTestInvalidResultError(
                message=f"Result value exceeds {_MAX_VALUE_LENGTH} characters (got {len(value_str)})",
            )

        return value_str

    @staticmethod
    def _build_response(value: str, query: str) -> Response:
        response_schema = cache_invalidation_test_schemas.CacheInvalidationTestResponseSchema()
        response_data = response_schema.dump(
            {
                "result": {
                    "value": value,
                    "query": query,
                },
            }
        )
        return web.json_response(response_data)

    async def _execute_formula_mode(self, cache_invalidation_source: CacheInvalidationSource) -> Response:
        field = cache_invalidation_source.field
        if field is None or not field.formula.strip():
            raise CacheInvalidationTestQueryError(message="Formula mode requires a field with a formula")

        original_result_schema = self.dataset.result_schema
        patched_result_schema = ResultSchema(fields=list(original_result_schema.fields) + [field])
        self.dataset.data.result_schema = patched_result_schema

        try:
            select_specs = [
                RawSelectFieldSpec(ref=IdFieldRef(id=field.guid)),
            ]

            filter_specs: list[RawFilterFieldSpec] = []
            for obligatory_filter in cache_invalidation_source.filters:
                for default_filter in obligatory_filter.default_filters:
                    filter_specs.append(
                        RawFilterFieldSpec(
                            ref=IdFieldRef(id=obligatory_filter.field_guid),
                            operation=default_filter.operation,
                            values=default_filter.values,
                        )
                    )

            raw_query_spec_union = RawQuerySpecUnion(
                select_specs=select_specs,
                filter_specs=filter_specs,
                meta=RawQueryMetaInfo(query_type=QueryType.result),
                limit=2,
                group_by_policy=GroupByPolicy.disable,
            )

            try:
                merged_stream = await self.execute_all_queries(
                    raw_query_spec_union=raw_query_spec_union,
                    autofill_legend=False,
                    call_post_exec_async_hook=False,
                    use_cache=False,
                )
            except Exception as ex:
                LOGGER.exception("Cache invalidation formula query execution failed", exc_info=True)
                raise CacheInvalidationTestQueryError(
                    message="Cache invalidation formula query execution failed",
                    debug_info={"db_message": str(ex)},
                ) from ex

            debug_query = ""
            if merged_stream.meta.blocks:
                debug_query = merged_stream.meta.blocks[0].debug_query or ""

            rows = list(merged_stream.rows)
            self._validate_single_row(row_count=len(rows))

            row_data = rows[0].data
            self._validate_single_column(row_data)

            value_str = self._validate_string_value(row_data[0])

        finally:
            self.dataset.data.result_schema = original_result_schema

        # 9. Build response
        return self._build_response(value=value_str, query=debug_query)

    async def _execute_sql_mode(self, sql_query: str) -> Response:
        ds_accessor = DatasetComponentAccessor(dataset=self.dataset)
        source_ids = ds_accessor.get_data_source_id_list()
        if not source_ids:
            raise CacheInvalidationTestQueryError(message="Dataset has no data sources")

        dsrc_coll_factory = DataSourceCollectionFactory(
            us_entry_buffer=self.dl_request.us_manager.get_entry_buffer(),
        )
        dsrc_coll_spec = ds_accessor.get_data_source_coll_spec_strict(source_id=source_ids[0])
        dsrc_coll = dsrc_coll_factory.get_data_source_collection(
            spec=dsrc_coll_spec,
            dataset_parameter_values={},
        )

        conn_id = dsrc_coll.effective_connection_id
        if conn_id is None:
            raise CacheInvalidationTestQueryError(message="Could not determine connection for data source")

        connection = await self.dl_request.us_manager.get_by_id(
            conn_id,
            ConnectionBase,
        )
        assert isinstance(connection, ConnectionBase)

        if isinstance(connection, RawSqlLevelConnectionMixin):
            if not connection.is_subselect_allowed:
                raise CacheInvalidationTestSubselectNotAllowedError()
        else:
            raise CacheInvalidationTestSubselectNotAllowedError(
                message="Connection type does not support raw SQL queries"
            )

        sa_query = sa_plain_text(sql_query)
        ce_query = ConnExecutorQuery(
            query=sa_query,
            debug_compiled_query=sql_query,
            trusted_query=False,
            autodetect_user_types=True,
        )

        sr = self.dl_request.services_registry
        ce_factory = sr.get_conn_executor_factory()
        conn_executor = ce_factory.get_async_conn_executor(connection)

        _MAX_ROWS_TO_FETCH = 3

        try:
            exec_result = await conn_executor.execute(ce_query)
            rows: list = []
            async for chunk in exec_result.result:
                rows.extend(chunk)
                if len(rows) >= _MAX_ROWS_TO_FETCH:
                    break
        except Exception as ex:
            LOGGER.exception("Cache invalidation SQL query execution failed")
            raise CacheInvalidationTestQueryError(
                message="Cache invalidation query execution failed",
                debug_info={"db_message": str(ex)},
            ) from ex

        user_types = exec_result.user_types
        if user_types and len(user_types) > 0 and user_types[0] != UserDataType.string:
            raise CacheInvalidationTestNonStringResultError(
                message=f"Expected string result type, got {user_types[0].name}",
            )

        self._validate_single_row(row_count=len(rows))

        row = rows[0]
        self._validate_single_column(row)

        value = self._validate_string_value(row[0])

        return self._build_response(value=value, query=sql_query)
