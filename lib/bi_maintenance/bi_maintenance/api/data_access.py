"""
Usage:

from bi_core.maintenance.helpers import get_sync_usm, get_dataset
from bi_api_lib.maintenance.data_access import get_result_data, make_query_spec_union

ds = get_dataset('hfu4hg98wh48', is_async_env=True)
raw_query_spec_union = make_query_spec_union(select=['My Field', 'fd83956d-8da7-4908-b376-ffc11576583a'])
us_manager = get_sync_usm()
response_json = await get_result_data(dataset=ds, us_manager=us_manager, raw_query_spec=raw_query_spec)
print_result_data(response_json)

"""

from typing import Iterable, Union, Optional

import tabulate

from bi_constants.enums import PivotRole

from bi_api_commons.base_models import RequestContextInfo
from bi_api_commons.reporting.registry import DefaultReportingRegistry, ReportingRegistry
from bi_core.us_dataset import Dataset
from bi_core.us_manager.us_manager import USManagerBase

from bi_api_lib.dataset.view import DatasetView
from bi_api_lib.query.formalization.raw_specs import (
    IdOrTitleFieldRef, RawSelectFieldSpec, RawQuerySpecUnion,
)
from bi_api_lib.query.formalization.raw_pivot_specs import (
    RawPivotLegendItem, RawPivotSpec, RawDimensionPivotRoleSpec, RawPivotMeasureRoleSpec,
)
from bi_api_lib.query.formalization.legend_formalizer import ResultLegendFormalizer
from bi_query_processing.legend.block_legend import BlockSpec
from bi_api_lib.query.formalization.block_formalizer import BlockFormalizer
from bi_api_lib.query.formalization.pivot_formalizer import PivotFormalizer
from bi_query_processing.postprocessing.primitives import PostprocessedQueryUnion, PostprocessedQueryBlock
from bi_query_processing.postprocessing.postprocessor import DataPostprocessor
from bi_query_processing.merging.merger import DataStreamMerger
from bi_api_lib.api_common.data_serialization import (
    DataRequestResponseSerializer, PivotDataRequestResponseSerializer,
)
from bi_api_lib.request_model.normalization.drm_normalizer_pivot import PivotSpecNormalizer
from bi_query_processing.merging.primitives import MergedQueryDataStream
from bi_api_lib.pivot.pandas.transformer import PdPivotTransformer


def get_ds_view(
        dataset: Dataset,
        us_manager: USManagerBase,
        block_spec: BlockSpec,
        rci: Optional[RequestContextInfo] = None,
) -> DatasetView:
    ds_view = DatasetView(
        ds=dataset, us_manager=us_manager,
        block_spec=block_spec,
        rci=rci or RequestContextInfo.create_empty(),
    )
    return ds_view


def make_query_spec_union(
        select: Union[list[str], dict[str, int]], disable_rls: bool = True,
) -> RawQuerySpecUnion:
    if not isinstance(select, dict):
        select = {item: idx for idx, item in enumerate(select)}
    assert isinstance(select, dict)
    return RawQuerySpecUnion(
        select_specs=[
            RawSelectFieldSpec(
                ref=IdOrTitleFieldRef(id_or_title=item),
                legend_item_id=liid,
            )
            for item, liid in select.items()
        ],
        disable_rls=disable_rls,
    )


def make_pivot_spec(
        row_dimensions: Iterable[str],
        column_dimensions: Iterable[str],
        measures: Iterable[str],
        normalize: bool = True,
) -> tuple[RawQuerySpecUnion, RawPivotSpec]:

    row_dimensions = list(row_dimensions)
    column_dimensions = list(column_dimensions)
    measures = list(measures)

    select = {
        item: idx for idx, item in enumerate(row_dimensions + column_dimensions + measures)
    }
    raw_query_spec_union = make_query_spec_union(select=select)
    pivot_items = [
        *(
            RawPivotLegendItem(
                legend_item_ids=[select[item]],
                role_spec=RawDimensionPivotRoleSpec(role=PivotRole.pivot_row),
            )
            for item in row_dimensions
        ),
        *(
            RawPivotLegendItem(
                legend_item_ids=[select[item]],
                role_spec=RawDimensionPivotRoleSpec(role=PivotRole.pivot_column),
            )
            for item in column_dimensions
        ),
        *(
            RawPivotLegendItem(
                legend_item_ids=[select[item]],
                role_spec=RawPivotMeasureRoleSpec(role=PivotRole.pivot_measure),
            )
            for item in measures
        ),
    ]
    raw_pivot_spec = RawPivotSpec(structure=pivot_items)

    if normalize:
        normalizer = PivotSpecNormalizer()
        raw_pivot_spec, raw_query_spec_union = normalizer.normalize_spec(
            spec=raw_pivot_spec, raw_query_spec_union=raw_query_spec_union
        )

    return raw_query_spec_union, raw_pivot_spec


async def get_merged_data_stream(
        dataset: Dataset,
        us_manager: USManagerBase,
        raw_query_spec_union: RawQuerySpecUnion,
        allow_query_cache_usage: bool = True,
        reporting_registry: Optional[ReportingRegistry] = None,
) -> MergedQueryDataStream:
    profiler_prefix = 'maintenance-result'

    legend_formalizer = ResultLegendFormalizer(dataset=dataset)
    legend = legend_formalizer.make_legend(raw_query_spec_union=raw_query_spec_union)
    block_formalizer = BlockFormalizer(dataset=dataset, reporting_registry=reporting_registry)
    block_legend = block_formalizer.make_block_legend(
        raw_query_spec_union=raw_query_spec_union, legend=legend,
    )
    rci = reporting_registry.rci if reporting_registry is not None else RequestContextInfo.create_empty()
    ds_view = get_ds_view(dataset=dataset, us_manager=us_manager, block_spec=block_legend.blocks[0], rci=rci)
    exec_info = ds_view.build_exec_info()
    executed_query = await ds_view.get_data_async(
        exec_info=exec_info,
        allow_cache_usage=allow_query_cache_usage,
    )
    postprocessor = DataPostprocessor(profiler_prefix=profiler_prefix)
    postprocessed_query = postprocessor.get_postprocessed_data(
        executed_query=executed_query,
        block_spec=block_legend.blocks[0],
    )
    postprocessed_query_union = PostprocessedQueryUnion(
        blocks=[PostprocessedQueryBlock.from_block_spec(
            block_spec=block_legend.blocks[0], postprocessed_query=postprocessed_query,
        )],
        legend=legend,
        limit=raw_query_spec_union.limit,
        offset=raw_query_spec_union.offset,
    )
    merged_stream = DataStreamMerger().merge(postprocessed_query_union=postprocessed_query_union)
    return merged_stream


async def get_result_data(
    dataset: Dataset,
    us_manager: USManagerBase,
    raw_query_spec_union: RawQuerySpecUnion,
    allow_query_cache_usage: bool = True
) -> dict:

    merged_stream = await get_merged_data_stream(
        dataset=dataset, us_manager=us_manager,
        raw_query_spec_union=raw_query_spec_union,
        allow_query_cache_usage=allow_query_cache_usage,
    )
    response_json = DataRequestResponseSerializer.make_data_response_v1(merged_stream=merged_stream)
    return response_json


async def get_pivot_data(
        dataset: Dataset,
        us_manager: USManagerBase,
        raw_query_spec_union: RawQuerySpecUnion,
        raw_pivot_spec: RawPivotSpec,
        allow_query_cache_usage: bool = True
) -> dict:
    reporting_registry = DefaultReportingRegistry(rci=RequestContextInfo.create_empty())
    merged_stream = await get_merged_data_stream(
        dataset=dataset, us_manager=us_manager,
        raw_query_spec_union=raw_query_spec_union,
        allow_query_cache_usage=allow_query_cache_usage,
        reporting_registry=reporting_registry,
    )
    pivot_formalizer = PivotFormalizer(dataset=dataset, legend=merged_stream.legend)
    pivot_legend = pivot_formalizer.make_pivot_legend(raw_pivot_spec=raw_pivot_spec)
    pivot_transformer = PdPivotTransformer(legend=merged_stream.legend, pivot_legend=pivot_legend)
    pivot_table = pivot_transformer.pivot(rows=merged_stream.rows)
    response_json = PivotDataRequestResponseSerializer.make_pivot_response(
        merged_stream=merged_stream, pivot_table=pivot_table, reporting_registry=reporting_registry,
    )
    return response_json


def print_result_data(response_json: dict) -> None:
    result_data = response_json['result']['data']
    headers = [item[0] for item in result_data['Type'][1][1]]
    data = result_data['Data']
    print(tabulate.tabulate(data, headers=headers, tablefmt='pipe'))
