from __future__ import annotations

import abc
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
)

from aiohttp import web

from dl_api_lib.app.data_api.resources.base import (
    RequiredResourceDSAPI,
    requires,
)
from dl_api_lib.app.data_api.resources.dataset.base import DatasetDataBaseView
import dl_api_lib.schemas.data
import dl_api_lib.schemas.main
from dl_app_tools.profiling_base import generic_profiler_async
from dl_query_processing.merging.primitives import MergedQueryDataStream


if TYPE_CHECKING:
    from aiohttp.web_response import Response

    from dl_api_lib.request_model.data import DataRequestModel

LOGGER = logging.getLogger(__name__)


class DatasetDistinctView(DatasetDataBaseView, abc.ABC):
    endpoint_code = "DatasetVersionValuesDistinct"
    # TODO FIX: Move to constants
    profiler_prefix = "distinct"

    # TODO FIX: Move request deserialization logic to schema
    # TODO FIX: Move response serialization logic to schema
    # TODO FIX: Add docs/schemas decorator
    # @schematic_request(
    #     ns=ns, body=dl_api_lib.schemas.data.DatasetVersionValuesDistinctPostSchema(),
    #     responses={200: ('Success', dl_api_lib.schemas.data.DatasetVersionValuesDistinctResponseSchema())}
    # )
    @generic_profiler_async("ds-values-distinct-full")
    @DatasetDataBaseView.with_resolved_entities
    @requires(RequiredResourceDSAPI.JSON_REQUEST)
    async def post(self) -> Response:
        req_model: DataRequestModel = self.load_req_model()
        self.check_dataset_revision_id(req_model)

        enable_mutation_caching = self.dl_request.services_registry.get_mutation_cache_factory() is not None
        await self.prepare_dataset_for_request(req_model=req_model, enable_mutation_caching=enable_mutation_caching)

        merged_stream = await self.execute_all_queries(
            raw_query_spec_union=req_model.raw_query_spec_union,
            autofill_legend=req_model.autofill_legend,
            call_post_exec_async_hook=False,
        )

        response_json = self.make_response(req_model=req_model, merged_stream=merged_stream)
        return web.json_response(response_json)

    @abc.abstractmethod
    def load_req_model(self) -> DataRequestModel:
        raise NotImplementedError()

    @abc.abstractmethod
    def make_response(
        self,
        req_model: DataRequestModel,
        merged_stream: MergedQueryDataStream,
    ) -> Dict[str, Any]:
        raise NotImplementedError()


class DatasetDistinctViewV1(DatasetDistinctView):
    """
    Old API v1 format (input and output)
    """

    def load_req_model(self) -> DataRequestModel:
        schema = dl_api_lib.schemas.data.DatasetVersionValuesDistinctPostSchema()
        req_model: DataRequestModel = schema.load(self.dl_request.json)
        return req_model

    def make_response(
        self,
        req_model: DataRequestModel,
        merged_stream: MergedQueryDataStream,
    ) -> Dict[str, Any]:
        return self._make_response_v1(req_model=req_model, merged_stream=merged_stream)


class DatasetDistinctViewV1_5(DatasetDistinctView):
    """
    Same as v1, for full v1.5 coverage
    """

    def load_req_model(self) -> DataRequestModel:
        schema = dl_api_lib.schemas.data.DistinctDataRequestV2Schema()
        req_model: DataRequestModel = schema.load(self.dl_request.json)
        return req_model

    def make_response(
        self,
        req_model: DataRequestModel,
        merged_stream: MergedQueryDataStream,
    ) -> Dict[str, Any]:
        return self._make_response_v1(req_model=req_model, merged_stream=merged_stream)


class DatasetDistinctViewV2(DatasetDistinctView):
    """
    New API v2 format (input and output)
    """

    def load_req_model(self) -> DataRequestModel:
        schema = dl_api_lib.schemas.data.DistinctDataRequestV2Schema()
        req_model: DataRequestModel = schema.load(self.dl_request.json)
        return req_model

    def make_response(
        self,
        req_model: DataRequestModel,
        merged_stream: MergedQueryDataStream,
    ) -> Dict[str, Any]:
        return self._make_response_v2(merged_stream=merged_stream)
