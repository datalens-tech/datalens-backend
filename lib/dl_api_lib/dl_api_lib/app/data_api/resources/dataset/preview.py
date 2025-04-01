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
import dl_api_lib.schemas.data
import dl_api_lib.schemas.main
from dl_app_tools.profiling_base import generic_profiler_async
from dl_constants.enums import DataSourceRole
from dl_core.components.accessor import DatasetComponentAccessor
from dl_core.data_source.collection import DataSourceCollectionFactory
from dl_core.dataset_capabilities import DatasetCapabilities
import dl_core.exc as common_exc
from dl_query_processing.legend.block_legend import BlockSpec
from dl_query_processing.merging.primitives import MergedQueryDataStream
from dl_query_processing.postprocessing.primitives import PostprocessedQuery


if TYPE_CHECKING:
    from aiohttp.web_response import Response

    from dl_api_lib.request_model.data import DataRequestModel
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
    @DatasetDataBaseView.with_resolved_entities
    @requires(RequiredResourceDSAPI.JSON_REQUEST)
    async def post(self) -> Response:
        req_model = self.load_req_model()

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
    ) -> PostprocessedQuery:
        ds_accessor = DatasetComponentAccessor(dataset=self.dataset)
        if not ds_accessor.get_data_source_id_list():
            return PostprocessedQuery()

        return await super().execute_query(
            block_spec=block_spec,
            profiling_postfix=profiling_postfix,
        )

    def load_req_model(self) -> DataRequestModel:
        schema = dl_api_lib.schemas.data.DatasetPreviewRequestSchema()
        req_model: DataRequestModel = schema.load(self.dl_request.json)
        return req_model

    @abc.abstractmethod
    def make_response(
        self,
        req_model: DataRequestModel,
        merged_stream: MergedQueryDataStream,
    ) -> dict[str, Any]:
        raise NotImplementedError()


class DatasetPreviewViewV1(DatasetPreviewView):
    """
    Old API v1 format (input and output)
    """

    def make_response(
        self,
        req_model: DataRequestModel,
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
        req_model: DataRequestModel,
        merged_stream: MergedQueryDataStream,
    ) -> dict[str, Any]:
        return self._make_response_v2(merged_stream=merged_stream)
