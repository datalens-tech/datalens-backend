from __future__ import annotations

import abc
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Collection,
    Optional,
)

from aiohttp import web

from dl_api_lib.app.data_api.resources.base import (
    RequiredResourceDSAPI,
    requires,
)
from dl_api_lib.app.data_api.resources.dataset.base import DatasetDataBaseView
from dl_api_lib.dataset.utils import invalidate_sample_sources
from dl_api_lib.enums import USPermissionKind
from dl_api_lib.exc import PreviewSourceModificationNotAllowedError
import dl_api_lib.schemas.data
import dl_api_lib.schemas.main
from dl_api_lib.utils.base import check_permission_on_entry
from dl_app_tools.profiling_base import generic_profiler_async
from dl_constants.enums import (
    DataSourceRole,
    DataSourceType,
)
from dl_core.base_models import DefaultConnectionRef
from dl_core.components.accessor import DatasetComponentAccessor
from dl_core.data_source.collection import DataSourceCollectionFactory
from dl_core.data_source.utils import get_parameters_hash
from dl_core.dataset_capabilities import DatasetCapabilities
import dl_core.exc as common_exc
from dl_query_processing.compilation.specs import ParameterValueSpec
from dl_query_processing.legend.block_legend import BlockSpec
from dl_query_processing.merging.primitives import MergedQueryDataStream
from dl_query_processing.postprocessing.primitives import PostprocessedQuery


if TYPE_CHECKING:
    from aiohttp.web_response import Response

    from dl_api_lib.request_model.data import PreviewDataRequestModel
    from dl_core.us_dataset import Dataset


LOGGER = logging.getLogger(__name__)


class DatasetPreviewView(DatasetDataBaseView, abc.ABC):
    endpoint_code = "DatasetVersionPreview"
    # TODO FIX: Move to constants
    profiler_prefix = "preview"

    STORED_DATASET_REQUIRED = False

    def resolve_dataset_source_role(self, dataset: Dataset, log_reasons: bool = False) -> DataSourceRole:
        dsrc_coll_factory = DataSourceCollectionFactory(us_entry_buffer=self.dl_request.us_manager.get_entry_buffer())
        capabilities = DatasetCapabilities(dataset=dataset, dsrc_coll_factory=dsrc_coll_factory)
        try:
            return capabilities.resolve_source_role(for_preview=True, log_reasons=log_reasons)
        except common_exc.NoCommonRoleError as e:
            raise common_exc.TableNameNotConfiguredError(
                "Dataset's sources are not configured correctly. Direct access is not possible"
            ) from e

    # TODO FIX: Add docs/schemas decorator
    # @schematic_request(
    #     ns=ns, body=dl_api_lib.schemas.data.DatasetPreviewRequestSchema(),
    #     responses={200: ('Success', dl_api_lib.schemas.data.DatasetVersionResultResponseSchema())}
    # )
    @generic_profiler_async("ds-preview-full")
    @DatasetDataBaseView.with_dataset_us_context
    @DatasetDataBaseView.with_resolved_entities
    @requires(RequiredResourceDSAPI.JSON_REQUEST)
    async def post(self) -> Response:
        req_model = self.load_req_model()

        req_model = await self._enforce_request_dataset_permissions(req_model)

        update_info = await self.prepare_dataset_for_request(
            req_model=req_model,
        )

        new_sources = update_info.added_own_source_ids + update_info.updated_own_source_ids
        invalidate_sample_sources(
            dataset=self.dataset,
            source_ids=new_sources,
            us_manager=self.dl_request.us_manager,
        )

        merged_stream = await self.execute_all_queries(
            raw_query_spec_union=req_model.raw_query_spec_union,
            autofill_legend=req_model.autofill_legend,
            call_post_exec_async_hook=False,
        )

        response_json = self.make_response(req_model=req_model, merged_stream=merged_stream)
        return web.json_response(response_json)

    async def execute_query(
        self,
        block_spec: BlockSpec,
        possible_data_lengths: Optional[Collection] = None,
        profiling_postfix: str = "",
        parameter_value_specs: list[ParameterValueSpec] | None = None,
        allow_cache_usage: bool | None = None,
        cache_invalidation_payload: str | None = None,
    ) -> PostprocessedQuery:
        ds_accessor = DatasetComponentAccessor(dataset=self.dataset)
        if not ds_accessor.get_data_source_id_list():
            return PostprocessedQuery()

        return await super().execute_query(
            block_spec=block_spec,
            profiling_postfix=profiling_postfix,
            parameter_value_specs=parameter_value_specs,
            cache_invalidation_payload=cache_invalidation_payload,
            allow_cache_usage=allow_cache_usage,
        )

    def load_req_model(self) -> PreviewDataRequestModel:
        schema = dl_api_lib.schemas.data.DatasetPreviewRequestSchema()
        req_model: PreviewDataRequestModel = schema.load(self.dl_request.json)
        return req_model

    async def _enforce_request_dataset_permissions(self, req_model: PreviewDataRequestModel) -> PreviewDataRequestModel:
        """Apply BI-7288 permission rules to the ``dataset`` field of the request body.

        - `dataset->view` only — drop the body's `dataset` patch silently;
          the saved US version is previewed as-is.
        - `dataset->edit` and `connection->view` on every used connection —
          accept the body's dataset patch unchanged (existing behavior).
        - `dataset->edit` but at least one connection lacks `view` (e.g. only
          `execute`) — source params must match the saved dataset; raise
          ``PreviewSourceModificationNotAllowedError`` otherwise. Field and
          other non-source changes remain allowed.
        """
        dataset_data = req_model.dataset
        if dataset_data is None:
            return req_model

        if not self.is_dataset_edit_allowed():
            LOGGER.info("caller lacks dataset->edit; dropping body dataset patch, previewing saved version")
            return req_model.clone(dataset=None)

        body_sources = dataset_data.get("sources") or []
        if not body_sources:
            return req_model

        us_manager = self.dl_request.us_manager
        for source in body_sources:
            connection_id = source.get("connection_id")
            if connection_id is None:
                continue
            # `with_resolved_entities` pre-loads connections only for the saved dataset;
            # body may reference new ones (e.g. replace-connection flow), so ensure first.
            await us_manager.ensure_connection_preloaded(DefaultConnectionRef(conn_id=connection_id))
            connection = us_manager.get_loaded_us_connection(connection_id)
            if connection.permissions is None or not check_permission_on_entry(connection, USPermissionKind.read):
                LOGGER.info(
                    "caller lacks connection->view on %s; validating sources match saved",
                    connection_id,
                )
                self._validate_request_sources_match_saved(body_sources)
                return req_model

        LOGGER.info("caller has dataset->edit and connection->view on all sources; accepting body patch")
        return req_model

    def _validate_request_sources_match_saved(self, body_sources: list[dict[str, Any]]) -> None:
        saved_accessor = DatasetComponentAccessor(dataset=self.dataset)
        saved_source_ids = set(saved_accessor.get_data_source_id_list())
        body_source_ids = {source["id"] for source in body_sources}
        if saved_source_ids != body_source_ids:
            LOGGER.info(
                "rejecting body patch — source id set differs from saved (saved=%s, body=%s)",
                sorted(saved_source_ids),
                sorted(body_source_ids),
            )
            raise PreviewSourceModificationNotAllowedError()

        us_manager = self.dl_request.us_manager
        dsrc_coll_factory = DataSourceCollectionFactory(us_entry_buffer=us_manager.get_entry_buffer())

        for body_source in body_sources:
            saved_spec = saved_accessor.get_data_source_coll_spec_strict(source_id=body_source["id"])
            saved_coll = dsrc_coll_factory.get_data_source_collection(
                spec=saved_spec,
                dataset_parameter_values={},
            )
            saved_hash = saved_coll.get_param_hash()

            body_source_type = body_source["source_type"]
            if isinstance(body_source_type, str):
                body_source_type = DataSourceType(body_source_type)
            body_hash = get_parameters_hash(
                source_type=body_source_type,
                connection_id=body_source.get("connection_id"),
                **(body_source.get("parameters") or {}),
            )

            if saved_hash != body_hash:
                LOGGER.info(
                    "rejecting body patch — source %s param hash differs (body_source_type=%s, body_params_keys=%s)",
                    body_source["id"],
                    body_source_type,
                    sorted((body_source.get("parameters") or {}).keys()),
                )
                raise PreviewSourceModificationNotAllowedError()

    @abc.abstractmethod
    def make_response(
        self,
        req_model: PreviewDataRequestModel,
        merged_stream: MergedQueryDataStream,
    ) -> dict[str, Any]:
        raise NotImplementedError()


class DatasetPreviewViewV1(DatasetPreviewView):
    """
    Old API v1 format (input and output)
    """

    def make_response(
        self,
        req_model: PreviewDataRequestModel,
        merged_stream: MergedQueryDataStream,
    ) -> dict[str, Any]:
        return self._make_response_v1(req_model=req_model, merged_stream=merged_stream)


class DatasetPreviewViewV1_5(DatasetPreviewViewV1):
    """
    Same as v1, for full v1.5 coverage
    """


class DatasetPreviewViewV2(DatasetPreviewView):
    """
    New API v2 format (input and output)
    """

    def make_response(
        self,
        req_model: PreviewDataRequestModel,
        merged_stream: MergedQueryDataStream,
    ) -> dict[str, Any]:
        return self._make_response_v2(merged_stream=merged_stream)
