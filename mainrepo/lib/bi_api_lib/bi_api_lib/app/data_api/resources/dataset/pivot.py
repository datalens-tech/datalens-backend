from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from aiohttp import web

from bi_api_lib.api_common.data_serialization import PivotDataRequestResponseSerializer
from bi_api_lib.app.data_api.resources.base import (
    RequiredResourceDSAPI,
    requires,
)
from bi_api_lib.app.data_api.resources.dataset.base import DatasetDataBaseView
from bi_api_lib.pivot.pandas.transformer import PdPivotTransformer
from bi_api_lib.query.formalization.pivot_formalizer import PivotFormalizer
from bi_api_lib.query.formalization.pivot_legend import PivotLegend
from bi_api_lib.request_model.normalization.drm_normalizer_pivot import PivotRequestModelNormalizer
import bi_api_lib.schemas.data
import bi_api_lib.schemas.main
from bi_app_tools.profiling_base import (
    GenericProfiler,
    generic_profiler_async,
)
from bi_query_processing.merging.primitives import MergedQueryDataStream
from bi_utils.utils import exc_catch_awrap

if TYPE_CHECKING:
    from aiohttp.web_response import Response

    from bi_api_lib.pivot.table import PivotTable
    from bi_api_lib.query.formalization.raw_pivot_specs import PivotPaginationSpec
    from bi_api_lib.request_model.data import PivotDataRequestModel
    from bi_query_processing.legend.field_legend import Legend


LOGGER = logging.getLogger(__name__)


class DatasetPivotView(DatasetDataBaseView):
    endpoint_code = "DatasetVersionPivot"
    # TODO FIX: Move to constants
    profiler_prefix = "pivot"

    # TODO FIX: Move request deserialization logic to schema
    # TODO FIX: Move response serialization logic to schema
    # TODO FIX: Add docs/schemas decorator
    # @schematic_request(
    #     ns=ns, body=bi_api_lib.schemas.data.PivotDataRequestBaseSchema(),
    #     responses={200: ('Success', bi_api_lib.schemas.data.DatasetVersionResultResponseSchema())}
    # )
    @generic_profiler_async("ds-pivot-full")
    @exc_catch_awrap
    @DatasetDataBaseView.with_resolved_entities
    @requires(RequiredResourceDSAPI.JSON_REQUEST)
    async def post(self) -> Response:
        schema = bi_api_lib.schemas.data.PivotDataRequestBaseSchema()
        req_model: PivotDataRequestModel = schema.load(self.dl_request.json)

        req_normalizer = PivotRequestModelNormalizer()
        req_model = req_normalizer.normalize_drm(req_model)

        enable_mutation_caching = self.dl_request.services_registry.get_mutation_cache_factory() is not None
        await self.prepare_dataset_for_request(req_model=req_model, enable_mutation_caching=enable_mutation_caching)

        merged_stream = await self.execute_all_queries(
            raw_query_spec_union=req_model.raw_query_spec_union,
            autofill_legend=req_model.autofill_legend,
            call_post_exec_async_hook=True,
        )

        pivot_legend: PivotLegend
        pivot_formalizer = PivotFormalizer(dataset=self.dataset, legend=merged_stream.legend)
        pivot_legend = pivot_formalizer.make_pivot_legend(raw_pivot_spec=req_model.pivot)

        pivot_table = await self.pivot_data(
            merged_stream=merged_stream,
            legend=merged_stream.legend,
            pagination=req_model.pivot.pagination,
            pivot_legend=pivot_legend,
        )

        # Serialize to response
        response_json = self.make_pivot_response(merged_stream=merged_stream, pivot_table=pivot_table)
        return web.json_response(response_json)

    async def pivot_data(
        self,
        merged_stream: MergedQueryDataStream,
        legend: Legend,
        pagination: PivotPaginationSpec,
        pivot_legend: PivotLegend,
    ) -> PivotTable:
        """
        Async wrapper for CPU-intensive pivot table creation
        """
        executor = self.dl_request.services_registry.get_compute_executor()
        return await executor.execute(
            lambda: self._pivot_data_sync(
                merged_stream=merged_stream,
                legend=legend,
                pagination=pagination,
                pivot_legend=pivot_legend,
            )
        )

    def _pivot_data_sync(
        self,
        merged_stream: MergedQueryDataStream,
        legend: Legend,
        pagination: PivotPaginationSpec,
        pivot_legend: PivotLegend,
    ) -> PivotTable:
        pivot_op_name = "pivot"
        # Create pivot table
        with GenericProfiler(f"{self.profiler_prefix}-{pivot_op_name}-transform-full"):
            legend_data = bi_api_lib.schemas.legend.LegendSchema().dump(legend)
            LOGGER.info("Pivot legend", extra=dict(legend=legend_data))
            transformer = PdPivotTransformer(legend=legend, pivot_legend=pivot_legend)
            with GenericProfiler(f"{self.profiler_prefix}-{pivot_op_name}-transform"):
                pivot_table = transformer.pivot(rows=merged_stream.rows)

            # Sort the table
            with GenericProfiler(f"{self.profiler_prefix}-{pivot_op_name}-sort"):
                pivot_table.facade.sort()
            col_cnt, row_cnt = pivot_table.get_column_count(), pivot_table.get_row_count()
            LOGGER.info(
                "Pivot table has %s columns and %s rows",
                col_cnt,
                row_cnt,
                extra=dict(pivot_table_size=dict(columns=col_cnt, rows=row_cnt)),
            )

            # Paginate
            if pagination.non_default:
                with GenericProfiler(f"{self.profiler_prefix}-{pivot_op_name}-paginate"):
                    pivot_table.facade.paginate(
                        limit_rows=pagination.limit_rows,
                        offset_rows=pagination.offset_rows,
                    )

        return pivot_table

    def make_pivot_response(self, merged_stream: MergedQueryDataStream, pivot_table: PivotTable) -> dict:
        reporting_registry = (
            self.dl_request.services_registry.get_reporting_registry() if self.allow_notifications else None
        )
        return PivotDataRequestResponseSerializer.make_pivot_response(
            merged_stream=merged_stream,
            pivot_table=pivot_table,
            reporting_registry=reporting_registry,
        )
