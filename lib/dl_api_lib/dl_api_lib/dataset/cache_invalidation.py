import asyncio
import logging
from typing import (
    Any,
    Awaitable,
    Callable,
)

from dl_cache_engine.cache_invalidation import exc as cache_invalidation_exceptions
from dl_cache_engine.cache_invalidation.engine import TCacheInvalidationGenerateFunc
from dl_cache_engine.cache_invalidation.primitives import (
    CacheInvalidationEntry,
    CacheInvalidationKey,
)
from dl_cache_engine.processing_helper import CacheProcessingHelper
from dl_core.base_models import ConnCacheableDataModelMixin
from dl_core.cache_invalidation import CacheInvalidationError
from dl_core.components.accessor import DatasetComponentAccessor
from dl_core.connection_executors.common_base import ConnExecutorQuery
from dl_core.data_source.collection import DataSourceCollectionFactory
from dl_core.services_registry.top_level import ServicesRegistry
from dl_core.us_connection_base import (
    ConnectionBase,
    RawSqlLevelConnectionMixin,
)
from dl_core.us_dataset import Dataset
from dl_core.us_manager.us_manager_async import AsyncUSManager
from dl_core.utils import sa_plain_text


TValidateFunc = Callable[[], CacheInvalidationError | None]


LOGGER = logging.getLogger(__name__)


_INVALIDATION_QUERY_TIMEOUT_SEC: float = 20.0


_MAX_PAYLOAD_VALUE_LENGTH: int = 100


def _validate_payload_value(value: Any) -> CacheInvalidationEntry | None:
    if not isinstance(value, str):
        return CacheInvalidationEntry.from_exception(
            cache_invalidation_exceptions.CacheInvalidationNonStringResultError(
                message=f"Cache invalidation query returned {type(value).__name__}, expected string",
            )
        )
    if len(value) > _MAX_PAYLOAD_VALUE_LENGTH:
        return CacheInvalidationEntry.from_exception(
            cache_invalidation_exceptions.CacheInvalidationValueTooLongError(
                message=f"Cache invalidation value exceeds {_MAX_PAYLOAD_VALUE_LENGTH} characters (got {len(value)})",
            )
        )
    return None


def _extract_single_value_from_rows(rows: list) -> CacheInvalidationEntry:
    if len(rows) == 0:
        return CacheInvalidationEntry.from_exception(
            cache_invalidation_exceptions.CacheInvalidationEmptyResultError(
                message="Cache invalidation query returned no rows",
            )
        )

    if len(rows) > 1:
        return CacheInvalidationEntry.from_exception(
            cache_invalidation_exceptions.CacheInvalidationMultipleRowsError(
                message=f"Cache invalidation query returned {len(rows)} rows, expected exactly 1",
            )
        )

    row = rows[0]
    if len(row) == 0:
        return CacheInvalidationEntry.from_exception(cache_invalidation_exceptions.CacheInvalidationEmptyRowError())

    if len(row) > 1:
        return CacheInvalidationEntry.from_exception(
            cache_invalidation_exceptions.CacheInvalidationMultipleColumnsError(
                message=f"Cache invalidation query returned {len(row)} columns, expected exactly 1",
            )
        )

    payload = row[0]
    validation_entry = _validate_payload_value(payload)
    if validation_entry is not None:
        return validation_entry

    return CacheInvalidationEntry.from_data(data=payload)


async def get_invalidation_payload_sql(
    *,
    dataset: Dataset,
    ds_accessor: DatasetComponentAccessor,
    us_manager: AsyncUSManager,
    services_registry: ServicesRegistry,
    validate_func: TValidateFunc | None = None,
) -> str | None:
    cache_invalidation_source = dataset.data.cache_invalidation_source

    connection = await get_connection(ds_accessor=ds_accessor, us_manager=us_manager)
    if connection is None:
        return None

    if not isinstance(connection.data, ConnCacheableDataModelMixin):
        return None
    throttling_interval_sec = connection.data.cache_invalidation_throttling_interval_sec
    if throttling_interval_sec is None:
        return None

    inval_factory = services_registry.get_cache_invalidation_engine_factory()
    inval_engine = inval_factory.get_cache_engine()
    if inval_engine is None:
        LOGGER.debug("Invalidation cache engine is not available, skipping invalidation check")
        return None

    key = CacheInvalidationKey(
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

    helper = CacheProcessingHelper(
        cache_engine=None,
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
    execute_formula_func: Callable[[], Awaitable[list[list]]],
    validate_func: TValidateFunc | None = None,
) -> str | None:
    connection = await get_connection(ds_accessor=ds_accessor, us_manager=us_manager)
    if connection is None:
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

    key = CacheInvalidationKey(
        dataset_id=dataset.uuid or "",
        dataset_revision_id=dataset.revision_id or "",
        connection_id=connection.uuid or "",
        connection_revision_id=connection.revision_id or "",
    )

    async def generate_func() -> CacheInvalidationEntry:
        # Run pre-execution validation if provided
        if validate_func is not None:
            validation_error = validate_func()
            if validation_error is not None:
                LOGGER.warning(
                    "Formula invalidation validation failed: %s",
                    validation_error.message,
                )
                return CacheInvalidationEntry.from_exception(
                    cache_invalidation_exceptions.CacheInvalidationValidationError(
                        message=validation_error.message,
                        details={"title": validation_error.title, "locator": validation_error.locator},
                    )
                )

        try:
            rows = await asyncio.wait_for(
                execute_formula_func(),
                timeout=_INVALIDATION_QUERY_TIMEOUT_SEC,
            )
        except asyncio.TimeoutError:
            LOGGER.warning(
                "Formula invalidation query timed out after %.1f seconds",
                _INVALIDATION_QUERY_TIMEOUT_SEC,
            )
            return CacheInvalidationEntry.from_exception(
                cache_invalidation_exceptions.CacheInvalidationQueryTimeoutError(
                    message=f"Formula invalidation query timed out after {_INVALIDATION_QUERY_TIMEOUT_SEC}s",
                )
            )
        except Exception as exc:
            LOGGER.exception("Formula invalidation query failed")
            return CacheInvalidationEntry.from_exception(
                cache_invalidation_exceptions.CacheInvalidationQueryError(
                    message=str(exc),
                )
            )

        return _extract_single_value_from_rows(rows)

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
) -> TCacheInvalidationGenerateFunc:
    async def generate_func() -> CacheInvalidationEntry:
        if not isinstance(connection, RawSqlLevelConnectionMixin) or not connection.is_subselect_allowed:
            LOGGER.warning("SQL mode invalidation failed: subselect not allowed on connection")
            return CacheInvalidationEntry.from_exception(
                cache_invalidation_exceptions.CacheInvalidationSubselectNotAllowedError()
            )

        if validate_func is not None:
            validation_error = validate_func()
            if validation_error is not None:
                LOGGER.warning(
                    "SQL invalidation validation failed: %s",
                    validation_error.message,
                )
                return CacheInvalidationEntry.from_exception(
                    cache_invalidation_exceptions.CacheInvalidationValidationError(
                        message=validation_error.message,
                        details={"title": validation_error.title, "locator": validation_error.locator},
                    )
                )

        if not sql.strip():
            return CacheInvalidationEntry.from_exception(cache_invalidation_exceptions.CacheInvalidationEmptySqlError())

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
        except asyncio.TimeoutError:
            LOGGER.warning(
                "Cache invalidation SQL query timed out after %.1f seconds",
                _INVALIDATION_QUERY_TIMEOUT_SEC,
            )
            return CacheInvalidationEntry.from_exception(
                cache_invalidation_exceptions.CacheInvalidationQueryTimeoutError(
                    message=f"Cache invalidation query timed out after {_INVALIDATION_QUERY_TIMEOUT_SEC}s",
                )
            )
        except Exception as exc:
            LOGGER.exception("Cache invalidation SQL query execution failed")
            return CacheInvalidationEntry.from_exception(
                cache_invalidation_exceptions.CacheInvalidationQueryError(
                    message=str(exc),
                )
            )

        return _extract_single_value_from_rows(rows)

    return generate_func


async def get_connection(
    *,
    ds_accessor: DatasetComponentAccessor,
    us_manager: AsyncUSManager,
) -> ConnectionBase | None:
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
        connection = us_manager.get_loaded_us_connection(conn_id)
    except Exception:
        LOGGER.exception("Failed to get loaded connection %s for invalidation check", conn_id)
        return None

    return connection
