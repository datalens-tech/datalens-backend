from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Collection,
    Dict,
    Optional,
)

from aiohttp import web

from dl_api_lib.app.data_api.resources.base import (
    RequiredResourceDSAPI,
    requires,
)
from dl_api_lib.app.data_api.resources.dataset.base import DatasetDataBaseView
from dl_api_lib.dataset.view import DatasetView
import dl_api_lib.schemas.data
import dl_api_lib.schemas.main
from dl_app_tools.profiling_base import generic_profiler_async
from dl_query_processing.compilation.specs import ParameterValueSpec
from dl_query_processing.execution.primitives import (
    ExecutedQuery,
    ExecutedQueryMetaInfo,
)
from dl_query_processing.legend.block_legend import BlockSpec
from dl_query_processing.merging.primitives import MergedQueryDataStream
from dl_query_processing.postprocessing.postprocessor import DataPostprocessor
from dl_query_processing.postprocessing.primitives import PostprocessedQuery
from dl_query_processing.translation.primitives import DetailedType
from dl_utils.utils import enum_not_none


if TYPE_CHECKING:
    from aiohttp.web_response import Response

    from dl_api_lib.request_model.data import DataRequestModel


# TODO FIX: Generalize with sync version
class DatasetRangeView(DatasetDataBaseView, abc.ABC):
    endpoint_code = "DatasetVersionValuesRange"
    # TODO FIX: Move to constants
    profiler_prefix = "range"
    transpose_data: ClassVar[bool]

    # TODO FIX: Move request deserialization logic to schema
    # TODO FIX: Move response serialization logic to schema
    # TODO FIX: Add docs/schemas decorator
    # @schematic_request(
    #     ns=ns, body=dl_api_lib.schemas.data.DatasetVersionValuesRangePostSchema(),
    #     responses={200: ('Success', dl_api_lib.schemas.data.DatasetVersionValuesRangeResponseSchema())}
    # )
    @generic_profiler_async("ds-values-range-full")
    @DatasetDataBaseView.with_resolved_entities
    @requires(RequiredResourceDSAPI.JSON_REQUEST)
    async def post(self) -> Response:
        req_model = self.load_req_model()
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

    async def execute_query(
        self,
        block_spec: BlockSpec,
        possible_data_lengths: Optional[Collection] = None,
        profiling_postfix: str = "",
        parameter_value_specs: list[ParameterValueSpec] | None = None,
    ) -> PostprocessedQuery:
        us_manager = self.dl_request.us_manager

        ds_view = DatasetView(
            ds=self.dataset,
            us_manager=us_manager,
            block_spec=block_spec,
            rci=self.dl_request.rci,
            parameter_value_specs=parameter_value_specs,
        )

        # try getting cached values from source (MetricaAPI)
        field, min_value, max_value = ds_view.fast_get_expression_value_range()

        if min_value is not None and max_value is not None:
            field_id = field.guid
            meta_for_range = ExecutedQueryMetaInfo(
                phantom_select_ids=[],
                field_order=[(0, field_id), (1, field_id)],
                detailed_types=[
                    DetailedType(
                        field_id=field_id,
                        data_type=enum_not_none(field.data_type),
                        formula_data_type=None,
                        formula_data_type_params=None,
                    )
                    for _ in range(2)
                ],
            )
            executed_query = ExecutedQuery(rows=[[min_value, max_value]], meta=meta_for_range)
            postprocessor = DataPostprocessor(profiler_prefix=self.profiler_prefix)
            postprocessed_query = postprocessor.get_postprocessed_data(
                executed_query=executed_query,
                block_spec=block_spec,
            )

        else:
            postprocessed_query = await super().execute_query(
                block_spec=block_spec,
                profiling_postfix=profiling_postfix,
            )

        if self.transpose_data:
            # In v1 and v1.5 min and max are returned as two rows of the same column,
            # so the response has to be transposed
            postprocessed_query = self._transpose_range_data(postprocessed_query)
        return postprocessed_query

    def _transpose_range_data(self, postprocessed_query: PostprocessedQuery) -> PostprocessedQuery:
        min_value, max_value = next(iter(postprocessed_query.postprocessed_data))

        field_order = postprocessed_query.meta.field_order
        assert field_order is not None
        detailed_types = postprocessed_query.meta.detailed_types
        assert detailed_types is not None

        postprocessed_query = postprocessed_query.clone(
            postprocessed_data=[[min_value], [max_value]],
            meta=postprocessed_query.meta.clone(
                field_order=field_order[:1],
                detailed_types=detailed_types[:1],
            ),
        )
        return postprocessed_query


class DatasetRangeViewV1(DatasetRangeView):
    """
    Old API v1 format (input and output)
    """

    transpose_data = True

    def load_req_model(self) -> DataRequestModel:
        schema = dl_api_lib.schemas.data.DatasetVersionValuesRangePostSchema()
        req_model: DataRequestModel = schema.load(self.dl_request.json)
        return req_model

    def make_response(
        self,
        req_model: DataRequestModel,
        merged_stream: MergedQueryDataStream,
    ) -> Dict[str, Any]:
        return self._make_response_v1(req_model=req_model, merged_stream=merged_stream)


class DatasetRangeViewV1_5(DatasetRangeView):
    """
    Same as v1, for full v1.5 coverage
    """

    transpose_data = True

    def load_req_model(self) -> DataRequestModel:
        schema = dl_api_lib.schemas.data.RangeDataRequestV2Schema()
        req_model: DataRequestModel = schema.load(self.dl_request.json)
        return req_model

    def make_response(
        self,
        req_model: DataRequestModel,
        merged_stream: MergedQueryDataStream,
    ) -> Dict[str, Any]:
        return self._make_response_v1(req_model=req_model, merged_stream=merged_stream)


class DatasetRangeViewV2(DatasetRangeView):
    """
    New API v2 format (input and output)
    """

    transpose_data = False

    def load_req_model(self) -> DataRequestModel:
        schema = dl_api_lib.schemas.data.RangeDataRequestV2Schema()
        req_model: DataRequestModel = schema.load(self.dl_request.json)
        return req_model

    def make_response(
        self,
        req_model: DataRequestModel,
        merged_stream: MergedQueryDataStream,
    ) -> Dict[str, Any]:
        return self._make_response_v2(merged_stream=merged_stream)
