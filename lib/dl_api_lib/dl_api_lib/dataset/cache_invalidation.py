"""
Runtime cache invalidation helper for Data API.

Orchestrates the invalidation cache check before the main data query:
1. Determines if invalidation is enabled (connection + dataset config)
2. Builds the InvalidationCacheKey
3. Creates the generate_func (mode=sql or mode=formula)
4. Calls CacheProcessingHelper.get_cache_invalidation_result()
5. Returns the payload string (or None) to be added to the main cache key
"""

from __future__ import annotations

import asyncio
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Optional,
)

from dl_cache_engine.invalidation import (
    InvalidationCacheEntry,
    InvalidationCacheKey,
    TInvalidationGenerateFunc,
)
from dl_cache_engine.processing_helper import CacheProcessingHelper
from dl_constants.enums import CacheInvalidationMode
from dl_core.base_models import ConnCacheableDataModelMixin
from dl_core.cache_invalidation import CacheInvalidationError
from dl_core.connection_executors.common_base import ConnExecutorQuery
from dl_core.data_source.collection import DataSourceCollectionFactory
from dl_core.us_connection_base import (
    ConnectionBase,
    RawSqlLevelConnectionMixin,
)
from dl_core.utils import sa_plain_text


if TYPE_CHECKING:
    from dl_core.components.accessor import DatasetComponentAccessor
    from dl_core.services_registry.top_level import ServicesRegistry
    from dl_core.us_dataset import Dataset
    from dl_core.us_manager.us_manager_async import AsyncUSManager

# Callback type for pre-execution validation.
# Returns CacheInvalidationError if validation fails, None if OK.
TValidateFunc = Callable[[], CacheInvalidationError | None]


LOGGER = logging.getLogger(__name__)

# Timeout for the invalidation query execution (seconds).
# Invalidation queries should be lightweight; if they take longer than this,
# we treat it as an error and fall back to no-invalidation behavior.
_INVALIDATION_QUERY_TIMEOUT_SEC: float = 20.0

# Maximum allowed length for the invalidation payload value.
_MAX_PAYLOAD_VALUE_LENGTH: int = 100


def _make_entry_result(entry: InvalidationCacheEntry) -> tuple[bytes, InvalidationCacheEntry]:
    """Wrap an InvalidationCacheEntry into the (bytes, entry) tuple expected by RCL."""
    return entry.to_json_bytes(), entry


def _validate_payload_value(value: Any) -> InvalidationCacheEntry | None:
    if not isinstance(value, str):
        return InvalidationCacheEntry.make_error(
            error_code="NON_STRING_RESULT",
            error_message=f"Cache invalidation query returned {type(value).__name__}, expected string",
        )
    if len(value) > _MAX_PAYLOAD_VALUE_LENGTH:
        return InvalidationCacheEntry.make_error(
            error_code="VALUE_TOO_LONG",
            error_message=(
                f"Cache invalidation value exceeds {_MAX_PAYLOAD_VALUE_LENGTH} characters " f"(got {len(value)})"
            ),
        )
    return None


async def get_invalidation_payload_sql(
    *,
    dataset: Dataset,
    ds_accessor: DatasetComponentAccessor,
    us_manager: AsyncUSManager,
    services_registry: ServicesRegistry,
    validate_func: TValidateFunc | None = None,
) -> Optional[str]:
    """
    Check the invalidation cache for mode=sql and return the payload string, or None.

    Returns None when:
    - Invalidation is not configured (mode=off, no throttling, no Redis)
    - The invalidation query fails (graceful degradation)
    - mode=formula (must be handled separately via get_invalidation_payload_formula)

    ``validate_func`` is an optional callback that validates the SQL configuration
    before executing the query.  If it returns a ``CacheInvalidationError``, the
    error is written to Redis and the payload is ``None``.

    Writes errors to Redis so they can be shown to the user via the test endpoint.
    """
    cache_invalidation_source = dataset.data.cache_invalidation_source
    if cache_invalidation_source.mode != CacheInvalidationMode.sql:
        return None

    # Get the connection for the first data source
    connection = await _get_connection(ds_accessor=ds_accessor, us_manager=us_manager)
    if connection is None:
        return None

    if not connection.is_cache_invalidation_enabled:
        return None

    # Get throttling interval from connection
    if not isinstance(connection.data, ConnCacheableDataModelMixin):
        return None
    throttling_interval_sec = connection.data.cache_invalidation_throttling_interval_sec
    if throttling_interval_sec is None:
        return None

    # Get InvalidationCacheEngine from ServicesRegistry
    inval_factory = services_registry.get_cache_invalidation_engine_factory()
    inval_engine = inval_factory.get_cache_engine()
    if inval_engine is None:
        LOGGER.debug("Invalidation cache engine is not available, skipping invalidation check")
        return None

    # Build the invalidation cache key
    key = InvalidationCacheKey(
        dataset_id=dataset.uuid or "",
        dataset_revision_id=dataset.revision_id or "",
        connection_id=connection.uuid or "",
        connection_revision_id=connection.revision_id or "",
    )

    generate_func = _make_sql_generate_func(
        sql=cache_invalidation_source.sql or "",
        connection=connection,
        services_registry=services_registry,
        validate_func=validate_func,
    )

    # Create CacheProcessingHelper and check invalidation
    helper = CacheProcessingHelper(
        cache_engine=None,  # We don't need the main cache engine here
        cache_invalidation_engine=inval_engine,
    )

    try:
        payload = await helper.get_cache_invalidation_result(
            key=key,
            throttling_interval_sec=float(throttling_interval_sec),
            generate_func=generate_func,
        )
        return payload
    except Exception:
        LOGGER.exception("Error during invalidation cache check, skipping (graceful degradation)")
        return None


async def get_invalidation_payload_formula(
    *,
    dataset: Dataset,
    ds_accessor: DatasetComponentAccessor,
    us_manager: AsyncUSManager,
    services_registry: ServicesRegistry,
    execute_formula_func: Callable[[], Awaitable[Any]],
    validate_func: TValidateFunc | None = None,
) -> Optional[str]:
    """
    Check the invalidation cache for formula mode.

    ``execute_formula_func`` is an async callable that executes the formula query
    and returns the payload string. It is called only when the cache is stale.

    ``validate_func`` is an optional callback that validates the formula/filter
    configuration before executing the query.  If it returns a
    ``CacheInvalidationError``, the error is written to Redis and the payload
    is ``None``.
    """
    cache_invalidation_source = dataset.data.cache_invalidation_source
    if cache_invalidation_source.mode != CacheInvalidationMode.formula:
        return None

    connection = await _get_connection(ds_accessor=ds_accessor, us_manager=us_manager)
    if connection is None:
        return None

    if not connection.is_cache_invalidation_enabled:
        return None

    if not isinstance(connection.data, ConnCacheableDataModelMixin):
        return None
    throttling_interval_sec = connection.data.cache_invalidation_throttling_interval_sec
    if throttling_interval_sec is None:
        return None

    inval_factory = services_registry.get_cache_invalidation_engine_factory()
    inval_engine = inval_factory.get_cache_engine()
    if inval_engine is None:
        return None

    key = InvalidationCacheKey(
        dataset_id=dataset.uuid or "",
        dataset_revision_id=dataset.revision_id or "",
        connection_id=connection.uuid or "",
        connection_revision_id=connection.revision_id or "",
    )

    async def generate_func() -> tuple[bytes, InvalidationCacheEntry]:
        # Run pre-execution validation if provided
        if validate_func is not None:
            validation_error = validate_func()
            if validation_error is not None:
                LOGGER.warning(
                    "Formula invalidation validation failed: %s",
                    validation_error.message,
                )
                entry = InvalidationCacheEntry.make_error(
                    error_code="VALIDATION_ERROR",
                    error_message=validation_error.message,
                    error_details={"title": validation_error.title, "locator": validation_error.locator},
                )
                return _make_entry_result(entry)

        try:
            payload_str = await asyncio.wait_for(
                execute_formula_func(),
                timeout=_INVALIDATION_QUERY_TIMEOUT_SEC,
            )
            if payload_str is None:
                entry = InvalidationCacheEntry.make_error(
                    error_code="EMPTY_RESULT",
                    error_message="Formula invalidation query returned no result",
                )
                return _make_entry_result(entry)

            validation_entry = _validate_payload_value(payload_str)
            if validation_entry is not None:
                return _make_entry_result(validation_entry)

            entry = InvalidationCacheEntry.make_success(data=payload_str)
            return _make_entry_result(entry)
        except asyncio.TimeoutError:
            LOGGER.warning(
                "Formula invalidation query timed out after %.1f seconds",
                _INVALIDATION_QUERY_TIMEOUT_SEC,
            )
            entry = InvalidationCacheEntry.make_error(
                error_code="QUERY_TIMEOUT",
                error_message=f"Formula invalidation query timed out after {_INVALIDATION_QUERY_TIMEOUT_SEC}s",
            )
            return _make_entry_result(entry)
        except Exception as exc:
            LOGGER.exception("Formula invalidation query failed")
            entry = InvalidationCacheEntry.make_error(
                error_code="QUERY_ERROR",
                error_message=str(exc),
            )
            return _make_entry_result(entry)

    helper = CacheProcessingHelper(
        cache_engine=None,
        cache_invalidation_engine=inval_engine,
    )

    try:
        return await helper.get_cache_invalidation_result(
            key=key,
            throttling_interval_sec=float(throttling_interval_sec),
            generate_func=generate_func,
        )
    except Exception:
        LOGGER.exception("Error during formula invalidation cache check, skipping")
        return None


def _make_sql_generate_func(
    *,
    sql: str,
    connection: ConnectionBase,
    services_registry: ServicesRegistry,
    validate_func: TValidateFunc | None = None,
) -> TInvalidationGenerateFunc:
    """Create a generate_func for mode=sql that executes raw SQL via ConnExecutor."""

    async def generate_func() -> tuple[bytes, InvalidationCacheEntry]:
        # Check that raw SQL (subselect) is allowed on the connection
        if not isinstance(connection, RawSqlLevelConnectionMixin) or not connection.is_subselect_allowed:
            LOGGER.warning("SQL mode invalidation failed: subselect not allowed on connection")
            entry = InvalidationCacheEntry.make_error(
                error_code="SUBSELECT_NOT_ALLOWED",
                error_message="SQL mode cache invalidation requires subselect to be allowed on the connection",
            )
            return _make_entry_result(entry)

        # Run pre-execution validation if provided
        if validate_func is not None:
            validation_error = validate_func()
            if validation_error is not None:
                LOGGER.warning(
                    "SQL invalidation validation failed: %s",
                    validation_error.message,
                )
                entry = InvalidationCacheEntry.make_error(
                    error_code="VALIDATION_ERROR",
                    error_message=validation_error.message,
                    error_details={"title": validation_error.title, "locator": validation_error.locator},
                )
                return _make_entry_result(entry)

        if not sql.strip():
            entry = InvalidationCacheEntry.make_error(
                error_code="EMPTY_SQL",
                error_message="Cache invalidation SQL query is empty",
            )
            return _make_entry_result(entry)

        ce_factory = services_registry.get_conn_executor_factory()
        conn_executor = ce_factory.get_async_conn_executor(connection)

        sa_query = sa_plain_text(sql)
        ce_query = ConnExecutorQuery(
            query=sa_query,
            autodetect_user_types=True,
            trusted_query=False,
        )

        async def _execute_sql() -> list:
            exec_result = await conn_executor.execute(ce_query)
            rows: list = []
            async for chunk in exec_result.result:
                rows.extend(chunk)
                if len(rows) >= 2:
                    break
            return rows

        try:
            rows = await asyncio.wait_for(_execute_sql(), timeout=_INVALIDATION_QUERY_TIMEOUT_SEC)

            if len(rows) == 0:
                entry = InvalidationCacheEntry.make_error(
                    error_code="EMPTY_RESULT",
                    error_message="Cache invalidation query returned no rows",
                )
                return _make_entry_result(entry)

            if len(rows) > 1:
                entry = InvalidationCacheEntry.make_error(
                    error_code="MULTIPLE_ROWS",
                    error_message=f"Cache invalidation query returned {len(rows)} rows, expected exactly 1",
                )
                return _make_entry_result(entry)

            row = rows[0]
            if len(row) == 0:
                entry = InvalidationCacheEntry.make_error(
                    error_code="EMPTY_ROW",
                    error_message="Cache invalidation query returned a row with no columns",
                )
                return _make_entry_result(entry)

            if len(row) > 1:
                entry = InvalidationCacheEntry.make_error(
                    error_code="MULTIPLE_COLUMNS",
                    error_message=f"Cache invalidation query returned {len(row)} columns, expected exactly 1",
                )
                return _make_entry_result(entry)

            value = row[0]
            validation_entry = _validate_payload_value(value)
            if validation_entry is not None:
                return _make_entry_result(validation_entry)

            entry = InvalidationCacheEntry.make_success(data=value)
            return _make_entry_result(entry)

        except asyncio.TimeoutError:
            LOGGER.warning(
                "Cache invalidation SQL query timed out after %.1f seconds",
                _INVALIDATION_QUERY_TIMEOUT_SEC,
            )
            entry = InvalidationCacheEntry.make_error(
                error_code="QUERY_TIMEOUT",
                error_message=f"Cache invalidation query timed out after {_INVALIDATION_QUERY_TIMEOUT_SEC}s",
            )
            return _make_entry_result(entry)

        except Exception as exc:
            LOGGER.exception("Cache invalidation SQL query execution failed")
            entry = InvalidationCacheEntry.make_error(
                error_code="QUERY_ERROR",
                error_message=str(exc),
            )
            return _make_entry_result(entry)

    return generate_func


async def _get_connection(
    *,
    ds_accessor: DatasetComponentAccessor,
    us_manager: AsyncUSManager,
) -> Optional[ConnectionBase]:
    """Get the connection for the first data source of the dataset."""
    source_ids = ds_accessor.get_data_source_id_list()
    if not source_ids:
        return None

    dsrc_coll_factory = DataSourceCollectionFactory(
        us_entry_buffer=us_manager.get_entry_buffer(),
    )
    dsrc_coll_spec = ds_accessor.get_data_source_coll_spec_strict(source_id=source_ids[0])
    dsrc_coll = dsrc_coll_factory.get_data_source_collection(
        spec=dsrc_coll_spec,
        dataset_parameter_values={},
    )

    conn_id = dsrc_coll.effective_connection_id
    if conn_id is None:
        return None

    try:
        connection = await us_manager.get_by_id(conn_id, ConnectionBase)
    except Exception:
        LOGGER.exception("Failed to load connection %s for invalidation check", conn_id)
        return None

    if not isinstance(connection, ConnectionBase):
        return None

    return connection
