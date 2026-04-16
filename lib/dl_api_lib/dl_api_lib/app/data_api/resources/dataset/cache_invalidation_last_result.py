import datetime
import logging
from typing import Any

from aiohttp import web
from aiohttp.web_response import Response

from dl_api_lib import exc
from dl_api_lib.app.data_api.resources.dataset.base import DatasetDataBaseView
from dl_api_lib.enums import USPermissionKind
from dl_api_lib.schemas.cache_invalidation_last_result import CacheInvalidationLastResultResponseSchema
from dl_api_lib.utils.base import check_permission_on_entry
from dl_app_tools.profiling_base import generic_profiler_async
from dl_cache_engine.cache_invalidation.primitives import (
    CacheInvalidationEntry,
    CacheInvalidationErrorPayload,
    CacheInvalidationKey,
    CacheInvalidationStatus,
)
from dl_constants.enums import CacheInvalidationLastResultStatus
from dl_constants.exc import (
    DEFAULT_ERR_CODE_API_PREFIX,
    GLOBAL_ERR_PREFIX,
    DLBaseException,
)
from dl_core.base_models import ConnCacheableDataModelMixin
from dl_core.components.accessor import DatasetComponentAccessor
from dl_core.data_source.collection import DataSourceCollectionFactory
from dl_core.us_connection_base import ConnectionBase


LOGGER = logging.getLogger(__name__)

_response_schema = CacheInvalidationLastResultResponseSchema()


class DatasetCacheInvalidationLastResultView(DatasetDataBaseView):
    """
    Endpoint for retrieving the last cache invalidation result from Redis.

    GET /api/data/v2/datasets/{ds_id}/cache_invalidation_last_result

    Returns the last cached invalidation entry (success or error) without
    executing any queries. This is a read-only operation.

    Returns 400 when:
    - The user is not an editor of the dataset (risk of exposing RLS-restricted data).
    """

    endpoint_code = "DatasetCacheInvalidationLastResult"
    profiler_prefix = "cache-invalidation-last-result"

    STORED_DATASET_REQUIRED = True

    @staticmethod
    def _format_timestamp(executed_at: float) -> str:
        return datetime.datetime.fromtimestamp(executed_at, tz=datetime.timezone.utc).isoformat()

    def _exc_to_last_result_error(self, dl_exc: DLBaseException) -> dict[str, Any]:
        error_code = ".".join([GLOBAL_ERR_PREFIX, DEFAULT_ERR_CODE_API_PREFIX] + dl_exc.err_code)
        return {
            "code": error_code,
            "message": dl_exc.message,
            "details": dl_exc.details,
            "debug": dl_exc.debug_info,
        }

    @staticmethod
    def _entry_error_payload_to_last_result_error(payload: CacheInvalidationErrorPayload) -> dict[str, Any]:
        return {
            "code": payload.error_code,
            "message": payload.error_message,
            "details": payload.error_details,
            "debug": {},
        }

    def _make_response(
        self,
        status: CacheInvalidationLastResultStatus,
        last_result: str | None,
        timestamp: str | None,
        last_result_error: dict[str, Any] | None = None,
    ) -> Response:
        data = _response_schema.dump(
            {
                "status": status,
                "last_result": last_result,
                "timestamp": timestamp,
                "last_result_error": last_result_error,
            }
        )
        return web.json_response(data)

    def _make_error_response(
        self,
        dl_exc: DLBaseException,
        timestamp: str | None = None,
    ) -> Response:
        data = _response_schema.dump(
            {
                "status": CacheInvalidationLastResultStatus.error,
                "last_result": None,
                "timestamp": timestamp,
                "last_result_error": self._exc_to_last_result_error(dl_exc),
            }
        )
        return web.json_response(data)

    def _make_response_from_entry(self, entry: CacheInvalidationEntry) -> Response:
        timestamp = self._format_timestamp(entry.executed_at)

        if entry.status == CacheInvalidationStatus.SUCCESS:
            return self._make_response(
                status=CacheInvalidationLastResultStatus.success,
                last_result=entry.data,
                timestamp=timestamp,
            )

        assert isinstance(entry.payload, CacheInvalidationErrorPayload)
        return self._make_response(
            status=CacheInvalidationLastResultStatus.error,
            last_result=None,
            timestamp=timestamp,
            last_result_error=self._entry_error_payload_to_last_result_error(entry.payload),
        )

    @generic_profiler_async("ds-cache-invalidation-last-result")
    @DatasetDataBaseView.with_dataset_us_context
    @DatasetDataBaseView.with_resolved_entities
    async def get(self) -> Response:
        if not check_permission_on_entry(self.dataset, USPermissionKind.edit):
            raise exc.CacheInvalidationLastResultNotEditorError()

        ds_accessor = DatasetComponentAccessor(dataset=self.dataset)
        source_ids = ds_accessor.get_data_source_id_list()
        if not source_ids:
            return self._make_error_response(exc.CacheInvalidationLastResultNoSourcesError())

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
            return self._make_error_response(exc.CacheInvalidationLastResultNoConnectionError())

        connection = await self.dl_request.us_manager.get_by_id(conn_id, ConnectionBase)
        assert isinstance(connection, ConnectionBase)

        if not isinstance(connection.data, ConnCacheableDataModelMixin):
            return self._make_error_response(exc.CacheInvalidationLastResultNotSupportedError())

        if not connection.is_cache_invalidation_enabled:
            return self._make_error_response(exc.CacheInvalidationLastResultNotEnabledError())

        services_registry = self.dl_request.services_registry
        inval_factory = services_registry.get_cache_invalidation_engine_factory()
        inval_engine = inval_factory.get_cache_engine()
        if inval_engine is None:
            return self._make_error_response(exc.CacheInvalidationLastResultEngineUnavailableError())

        key = CacheInvalidationKey(
            dataset_id=self.dataset.uuid or "",
            dataset_revision_id=self.dataset.revision_id or "",
            connection_id=connection.uuid or "",
            connection_revision_id=connection.revision_id or "",
        )

        entry = await inval_engine.get_entry(key)
        if entry is None:
            return self._make_error_response(exc.CacheInvalidationLastResultNoResultError())

        return self._make_response_from_entry(entry)
