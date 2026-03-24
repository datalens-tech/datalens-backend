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
            return await self._execute_sql_mode(sql_query=cache_invalidation_source.sql or "")

        if cache_invalidation_source.mode == CacheInvalidationMode.formula:
            return await self._execute_formula_mode(cache_invalidation_source=cache_invalidation_source)

        raise CacheInvalidationTestError(
            message=f"Unsupported cache invalidation mode: {cache_invalidation_source.mode}"
        )

    async def _execute_formula_mode(self, cache_invalidation_source: CacheInvalidationSource) -> Response:
        """Execute formula-mode cache invalidation query and return the result.

        Steps:
        1. Temporarily add the CacheInvalidationField to the dataset's result_schema
        2. Build a RawQuerySpecUnion with the field as select and filters from cache_invalidation_source
        3. Execute via execute_all_queries (standard formula compilation + execution pipeline)
        4. Extract and validate the result
        5. Clean up the temporary field
        """
        field = cache_invalidation_source.field
        if field is None or not field.formula.strip():
            raise CacheInvalidationTestQueryError(message="Formula mode requires a field with a formula")

        # 1. Temporarily add the CacheInvalidationField to result_schema so the pipeline can find it
        result_schema = self.dataset.result_schema
        result_schema.fields.append(field)
        result_schema.reload_caches()

        try:
            # 2. Build select spec referencing the cache invalidation field by guid
            select_specs = [
                RawSelectFieldSpec(ref=IdFieldRef(id=field.guid)),
            ]

            # 3. Build filter specs from cache_invalidation_source.filters
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

            # 4. Build the query spec (limit=2 to detect >1 row results)
            raw_query_spec_union = RawQuerySpecUnion(
                select_specs=select_specs,
                filter_specs=filter_specs,
                meta=RawQueryMetaInfo(query_type=QueryType.result),
                limit=2,
                disable_rls=False,
                group_by_policy=GroupByPolicy.disable,
            )

            # 5. Execute via the standard pipeline
            try:
                merged_stream = await self.execute_all_queries(
                    raw_query_spec_union=raw_query_spec_union,
                    autofill_legend=False,
                    call_post_exec_async_hook=False,
                )
            except Exception as ex:
                LOGGER.exception("Cache invalidation formula query execution failed", exc_info=True)
                raise CacheInvalidationTestQueryError(
                    message="Cache invalidation formula query execution failed",
                    debug_info={"db_message": str(ex)},
                ) from ex

            # 6. Extract the debug query (actual SQL sent to the database)
            debug_query = ""
            if merged_stream.meta.blocks:
                debug_query = merged_stream.meta.blocks[0].debug_query or ""

            # 7. Collect rows from the stream
            rows = list(merged_stream.rows)

            if len(rows) == 0:
                raise CacheInvalidationTestInvalidResultError(
                    message="Query returned no rows",
                )

            if len(rows) > 1:
                raise CacheInvalidationTestInvalidResultError(
                    message=f"Expected exactly 1 row, got {len(rows)}",
                )

            row_data = rows[0].data
            if len(row_data) == 0:
                raise CacheInvalidationTestInvalidResultError(
                    message="Result row has no columns",
                )
            if len(row_data) > 1:
                raise CacheInvalidationTestInvalidResultError(
                    message=f"Expected exactly 1 column, got {len(row_data)}",
                )

            value = row_data[0]

            # 8. Validate that the result is a string
            if not isinstance(value, str):
                raise CacheInvalidationTestNonStringResultError(
                    message=f"Expected string result, got {type(value).__name__}",
                )

            value_str = str(value)
            if len(value_str) > _MAX_VALUE_LENGTH:
                raise CacheInvalidationTestInvalidResultError(
                    message=f"Result value exceeds {_MAX_VALUE_LENGTH} characters (got {len(value_str)})",
                )

        finally:
            # 9. Clean up: remove the temporary field from result_schema
            try:
                result_schema.fields.remove(field)
                result_schema.reload_caches()
            except ValueError:
                pass  # Field was already removed or not found

        # 10. Build response
        response_schema = cache_invalidation_test_schemas.CacheInvalidationTestResponseSchema()
        response_data = response_schema.dump(
            {
                "result": {
                    "value": value_str,
                    "query": debug_query,
                },
            }
        )
        return web.json_response(response_data)

    async def _execute_sql_mode(self, sql_query: str) -> Response:
        """Execute SQL-mode cache invalidation query and return the result."""

        # Get the first data source's connection
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

        # Check subselect support on the connection
        if isinstance(connection, RawSqlLevelConnectionMixin):
            if not connection.is_subselect_allowed:
                raise CacheInvalidationTestSubselectNotAllowedError()
        else:
            raise CacheInvalidationTestSubselectNotAllowedError(
                message="Connection type does not support raw SQL queries"
            )

        # Build and execute the query
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

        # Read at most a few rows to detect >1 row results without fetching the entire result set
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

        # Validate that the result type is string
        user_types = exec_result.user_types
        if user_types and len(user_types) > 0 and user_types[0] != UserDataType.string:
            raise CacheInvalidationTestNonStringResultError(
                message=f"Expected string result type, got {user_types[0].name}",
            )

        # Validate result
        if len(rows) == 0:
            raise CacheInvalidationTestInvalidResultError(
                message="Query returned no rows",
            )
        if len(rows) > 1:
            raise CacheInvalidationTestInvalidResultError(
                message=f"Expected exactly 1 row, got {len(rows)}",
            )

        row = rows[0]
        if len(row) == 0:
            raise CacheInvalidationTestInvalidResultError(
                message="Result row has no columns",
            )
        if len(row) > 1:
            raise CacheInvalidationTestInvalidResultError(
                message=f"Expected exactly 1 column, got {len(row)}",
            )

        # Additionally validate the actual Python type of the value
        # (user_types may be empty/None for some adapters)
        if not isinstance(row[0], str):
            raise CacheInvalidationTestNonStringResultError(
                message=f"Expected string result, got {type(row[0]).__name__}",
            )

        value = str(row[0])
        if len(value) > _MAX_VALUE_LENGTH:
            raise CacheInvalidationTestInvalidResultError(
                message=f"Result value exceeds {_MAX_VALUE_LENGTH} characters (got {len(value)})",
            )

        # Build response
        response_schema = cache_invalidation_test_schemas.CacheInvalidationTestResponseSchema()
        response_data = response_schema.dump(
            {
                "result": {
                    "value": value,
                    "query": sql_query,
                },
            }
        )
        return web.json_response(response_data)
