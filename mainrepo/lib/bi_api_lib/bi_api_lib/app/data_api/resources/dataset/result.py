from __future__ import annotations

import abc
import logging
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from aiohttp import web

from bi_constants.enums import FieldType, FieldRole, BIType

from bi_app_tools.profiling_base import generic_profiler_async

from bi_utils.utils import exc_catch_awrap

import bi_api_lib.schemas.data
import bi_api_lib.schemas.main

from bi_query_processing.enums import QueryType, EmptyQueryMode, GroupByPolicy
from bi_api_lib.app.data_api.resources.base import RequiredResourceDSAPI, requires
from bi_api_lib.app.data_api.resources.dataset.base import DatasetDataBaseView
from bi_api_lib.query.formalization.raw_specs import RawQuerySpecUnion
from bi_query_processing.legend.field_legend import (
    Legend, LegendItem, PlaceholderObjSpec, FieldObjSpec,
    TemplateRoleSpec, RoleSpec,
)
from bi_query_processing.legend.block_legend import BlockSpec
from bi_query_processing.postprocessing.primitives import PostprocessedRow
from bi_query_processing.merging.primitives import MergedQueryDataStream

if TYPE_CHECKING:
    from aiohttp.web_response import Response
    from bi_api_lib.request_model.data import DataRequestModel


LOGGER = logging.getLogger(__name__)


class DatasetResultView(DatasetDataBaseView, abc.ABC):
    endpoint_code = 'DatasetVersionResult'
    # TODO FIX: Move to constants
    profiler_prefix = 'result'

    # TODO FIX: Move request deserialization logic to schema
    # TODO FIX: Move response serialization logic to schema
    # TODO FIX: Add docs/schemas decorator
    # @schematic_request(
    #     ns=ns, body=bi_api_lib.schemas.data.DatasetVersionResultRequestSchema(),
    #     responses={200: ('Success', bi_api_lib.schemas.data.DatasetVersionResultResponseSchema())}
    # )
    @generic_profiler_async("ds-result-full")
    @exc_catch_awrap
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
            call_post_exec_async_hook=True,
        )

        totals, totals_query = None, None
        if req_model.with_totals:
            totals_query, totals = await self._make_totals(
                raw_query_spec_union=req_model.raw_query_spec_union,
                legend=merged_stream.legend,
            )

        response_json = self.make_response(
            req_model=req_model, merged_stream=merged_stream,
            totals=totals, totals_query=totals_query,
        )
        return web.json_response(response_json)

    @abc.abstractmethod
    def load_req_model(self) -> DataRequestModel:
        raise NotImplementedError

    @abc.abstractmethod
    def make_response(
            self, req_model: DataRequestModel, merged_stream: MergedQueryDataStream,
            totals_query: Optional[str], totals: Optional[PostprocessedRow],
    ) -> Dict[str, Any]:
        raise NotImplementedError()

    async def _make_totals(
            self, *, raw_query_spec_union: RawQuerySpecUnion, legend: Legend,
    ) -> Tuple[Optional[str], Optional[PostprocessedRow]]:
        block_id = 0
        assert all(item.block_id is None or item.block_id == block_id for item in legend.items)
        streamable_items = legend.list_streamable_items()

        # Patch legend by replacing all dimensions with placeholders
        updated_items: List[LegendItem] = []
        for item in streamable_items:
            if item.field_type == FieldType.DIMENSION:
                updated_item = item.clone(
                    obj=PlaceholderObjSpec(),
                    role_spec=TemplateRoleSpec(role=FieldRole.template, template=None),
                    data_type=BIType.string,
                )
            else:
                assert isinstance(item.obj, FieldObjSpec)
                updated_item = item.clone(
                    role_spec=RoleSpec(role=FieldRole.total)
                )
            updated_items.append(updated_item)

        # Add filters
        updated_items += legend.list_for_role(FieldRole.filter)

        legend_for_block = Legend(items=updated_items)

        block_spec = BlockSpec(  # Fake block spec just for use in the  postprocessing interface
            block_id=block_id, parent_block_id=None,
            legend_item_ids=[item.legend_item_id for item in streamable_items],
            legend=legend_for_block,
            query_type=QueryType.totals,
            group_by_policy=GroupByPolicy.force,
            ignore_nonexistent_filters=raw_query_spec_union.ignore_nonexistent_filters,
            disable_rls=raw_query_spec_union.disable_rls,
            allow_measure_fields=raw_query_spec_union.allow_measure_fields,
            require_obligatory_filters=raw_query_spec_union.require_obligatory_filters,
            empty_query_mode=EmptyQueryMode.empty_row,
        )
        postprocessed_query = await self.execute_query(
            block_spec=block_spec,
            possible_data_lengths=(0, 1),
        )

        first_item: Optional[PostprocessedRow] = None
        if postprocessed_query.postprocessed_data:
            first_item = postprocessed_query.postprocessed_data[0]

        return (
            postprocessed_query.meta.debug_query,
            first_item,
        )


class DatasetResultViewV1(DatasetResultView):
    """
    Old API v1 format (input and output)
    """

    def load_req_model(self) -> DataRequestModel:
        schema = bi_api_lib.schemas.data.DatasetVersionResultRequestSchema()
        req_model: DataRequestModel = schema.load(self.dl_request.json)
        return req_model

    def make_response(
        self, req_model: DataRequestModel, merged_stream: MergedQueryDataStream,
        totals_query: Optional[str], totals: Optional[PostprocessedRow],
    ) -> Dict[str, Any]:
        return self._make_response_v1(
            req_model=req_model, merged_stream=merged_stream,
            totals_query=totals_query, totals=totals,
        )


class DatasetResultViewV1_5(DatasetResultView):
    """
    Accepts requests in v2 format, but responds in old v1 format (with YQL schema).
    To make the transition smoother for the frontend.
    """

    def load_req_model(self) -> DataRequestModel:
        schema = bi_api_lib.schemas.data.ResultDataRequestV1_5Schema()
        req_model: DataRequestModel = schema.load(self.dl_request.json)
        return req_model

    def make_response(
        self, req_model: DataRequestModel, merged_stream: MergedQueryDataStream,
        totals_query: Optional[str], totals: Optional[PostprocessedRow],
    ) -> Dict[str, Any]:
        return self._make_response_v1(
            req_model=req_model, merged_stream=merged_stream,
            totals_query=totals_query, totals=totals,
        )


class DatasetResultViewV2(DatasetResultView):
    """
    New API v2 format (input and output)
    """

    def load_req_model(self) -> DataRequestModel:
        schema = bi_api_lib.schemas.data.ResultDataRequestV2Schema()
        req_model: DataRequestModel = schema.load(self.dl_request.json)
        return req_model

    def make_response(
            self, req_model: DataRequestModel, merged_stream: MergedQueryDataStream,
            totals_query: Optional[str], totals: Optional[PostprocessedRow],
    ) -> Dict[str, Any]:
        return self._make_response_v2(merged_stream=merged_stream)
