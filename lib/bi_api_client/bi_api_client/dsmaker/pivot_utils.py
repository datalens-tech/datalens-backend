from collections import defaultdict
from http import HTTPStatus
from itertools import chain
from typing import AbstractSet, Optional, Tuple

from bi_constants.enums import OrderDirection, FieldRole, PivotRole, FieldType, QueryItemRefType
from bi_constants.internal_constants import MEASURE_NAME_TITLE, DIMENSION_NAME_TITLE

from bi_core.reporting.notifications import NotificationType

from bi_api_client.dsmaker.primitives import (
    Dataset, RequestLegendItem, OrderedField,
    RequestPivotItem, PivotPagination, WhereClause,
    PivotTotals,
)
from bi_api_client.dsmaker.api.data_api import SyncHttpDataApiV2, HttpDataApiResponse
from bi_api_client.dsmaker.data_abstraction.primitives import DataItemTag, DataCellTuple, DataItem
from bi_api_client.dsmaker.data_abstraction.mapping_base import DataCellMapper1D
from bi_api_client.dsmaker.data_abstraction.mapping_comparator import SimpleMapper1DComparator
from bi_api_client.dsmaker.data_abstraction.result import ResultDataAbstraction
from bi_api_client.dsmaker.data_abstraction.pivot import PivotDataAbstraction
from bi_api_lib.pivot.primitives import PivotMeasureSorting


SPECIAL_TITLES = {
    MEASURE_NAME_TITLE,
    DIMENSION_NAME_TITLE,
}


def _get_regular_legend_for_pivot(
        dataset: Dataset,
        columns: list[str],
        rows: list[str],
        measures: list[str],
        annotations: Optional[list[str]] = None,
        totals: Optional[list[tuple[str, ...]]] = None,
) -> tuple[list[RequestLegendItem], dict[str, list[int]]]:

    totals = totals or []
    assert totals is not None

    # Make regular legend
    main_block_id = 0
    legend_item_id = 0
    legend: list[RequestLegendItem] = []
    liid_map: dict[str, list[int]] = defaultdict(list)
    all_titles = chain(rows, columns, measures, annotations or ())
    item: RequestLegendItem
    for title in all_titles:
        assert title != DIMENSION_NAME_TITLE
        if liid_map[title]:  # Don't duplicate fields
            continue
        if title == MEASURE_NAME_TITLE:
            item = dataset.measure_name_as_req_legend_item(
                role=FieldRole.row, legend_item_id=legend_item_id, block_id=main_block_id)
        else:
            item = dataset.find_field(title=title).as_req_legend_item(
                role=FieldRole.row, legend_item_id=legend_item_id, block_id=main_block_id)
        legend.append(item)
        liid_map[title].append(legend_item_id)
        legend_item_id += 1

        if totals and title != MEASURE_NAME_TITLE:
            field = dataset.find_field(title=title)
            for totals_idx, totals_set in enumerate(totals):
                block_id = main_block_id + 1 + totals_idx  # the first block for totals should be 1
                if field.type == FieldType.DIMENSION:
                    if title in totals_set:
                        item = field.as_req_legend_item(legend_item_id=legend_item_id, block_id=block_id)
                    else:
                        item = dataset.placeholder_as_req_legend_item(
                            legend_item_id=legend_item_id, block_id=block_id,
                            role=FieldRole.template, template='',
                        )
                else:
                    item = field.as_req_legend_item(
                        legend_item_id=legend_item_id, block_id=block_id,
                        role=FieldRole.total,
                    )

                legend.append(item)
                liid_map[title].append(legend_item_id)
                legend_item_id += 1

    return legend, liid_map


def get_pivot_response(
        dataset: Dataset,
        data_api: SyncHttpDataApiV2,
        columns: list[str],
        rows: list[str],
        measures: list[str],
        measures_sorting_settings: Optional[list[Optional[PivotMeasureSorting]]] = None,
        annotations: Optional[list[str]] = None,
        title_mapping: Optional[dict[str, str]] = None,
        totals: Optional[list[tuple[str, ...]]] = None,
        simple_totals: Optional[PivotTotals] = None,
        with_totals: Optional[bool] = None,
        order_fields: Optional[dict[str, Optional[OrderDirection]]] = None,
        filters: Optional[list[WhereClause]] = None,
        pivot_pagination: Optional[PivotPagination] = None,
        autofill_legend: bool = False,
) -> HttpDataApiResponse:

    title_mapping = title_mapping or {}
    assert title_mapping is not None

    order_fields = order_fields or {}
    assert order_fields is not None

    totals = totals or []
    assert totals is not None

    if measures_sorting_settings is None:
        measures_sorting_settings = [None] * len(measures)
    assert len(measures_sorting_settings) == len(measures)

    # Make regular legend
    legend, liid_map = _get_regular_legend_for_pivot(
        dataset=dataset, columns=columns, rows=rows, measures=measures,
        annotations=annotations, totals=totals,
    )

    def make_pivot_item(
            title: str,
            role: PivotRole,
            measure_sorting_settings: Optional[PivotMeasureSorting] = None
    ) -> RequestPivotItem:
        legend_item_ids = liid_map[title]
        remapped_title = title_mapping.get(title, title)
        if title == MEASURE_NAME_TITLE:
            return dataset.make_req_pivot_item(
                role=role, legend_item_ids=legend_item_ids, title=remapped_title)
        if role == PivotRole.pivot_annotation:
            return dataset.make_req_pivot_item(
                role=role, legend_item_ids=legend_item_ids, title=remapped_title,
                annotation_type='color')
        if role in (PivotRole.pivot_row, PivotRole.pivot_column):
            return dataset.make_req_pivot_item(
                role=role, legend_item_ids=legend_item_ids, title=remapped_title,
                direction=order_fields.get(title, OrderDirection.asc),
            )
        if role == PivotRole.pivot_measure:
            return dataset.make_req_pivot_item(
                role=role, legend_item_ids=legend_item_ids, title=remapped_title,
                measure_sorting_settings=measure_sorting_settings)
        return dataset.make_req_pivot_item(
            role=role, legend_item_ids=legend_item_ids, title=remapped_title)

    def order_by_item(title: str, direction: OrderDirection) -> OrderedField:
        field = dataset.find_field(title=title)
        if direction == OrderDirection.desc:
            return field.desc
        return field.asc

    pivot_resp = data_api.get_pivot(
        dataset=dataset,
        fields=legend,
        pivot_structure=[
            *[
                make_pivot_item(title=title, role=PivotRole.pivot_column)
                for title in columns
            ],
            *[
                make_pivot_item(title=title, role=PivotRole.pivot_row)
                for title in rows
            ],
            *[
                make_pivot_item(
                    title=title, role=PivotRole.pivot_measure, measure_sorting_settings=measure_sorting_settings
                )
                for title, measure_sorting_settings in zip(measures, measures_sorting_settings)
            ],
            *[
                make_pivot_item(title=title, role=PivotRole.pivot_annotation)
                for title in annotations or ()
            ],
        ],
        order_by=[
            order_by_item(title=title, direction=direction)
            for title, direction in (order_fields or {}).items()
        ],
        filters=filters,
        pivot_pagination=pivot_pagination,
        pivot_totals=simple_totals,
        with_totals=with_totals,
        autofill_legend=autofill_legend,
        fail_ok=True,
    )
    return pivot_resp


def check_pivot_response(
        dataset: Dataset,
        data_api: SyncHttpDataApiV2,
        columns: list[str],
        rows: list[str],
        measures: list[str],
        measures_sorting_settings: Optional[list[Optional[PivotMeasureSorting]]] = None,
        annotations: Optional[list[str]] = None,
        title_mapping: Optional[dict[str, str]] = None,
        totals: Optional[list[tuple[str, ...]]] = None,
        simple_totals: Optional[PivotTotals] = None,
        with_totals: Optional[bool] = None,
        order_fields: Optional[dict[str, Optional[OrderDirection]]] = None,
        check_totals: Optional[list[tuple[str, ...]]] = None,
        filters: Optional[list[WhereClause]] = None,
        autofill_legend: bool = False,
        custom_pivot_legend_check: Optional[list[Tuple[str, PivotRole]]] = None,
        min_col_cnt: Optional[int] = None,
        max_col_cnt: Optional[int] = None,
        min_row_cnt: Optional[int] = None,
        max_row_cnt: Optional[int] = None,
        min_value_cnt: Optional[int] = None,
        max_value_cnt: Optional[int] = None,
        expected_notifications: Optional[list[NotificationType]] = None,
) -> PivotDataAbstraction:

    title_mapping = title_mapping or {}
    assert title_mapping is not None

    def norm_title(title: str) -> str:
        return title_mapping.get(title, title)

    assert simple_totals is None or totals is None
    if check_totals is None:
        if with_totals:
            check_totals = [
                *([tuple(columns)] if rows else []),
                *([tuple(rows)] if columns else []),
                *([()] if rows and columns else []),
            ]
        if totals is not None:
            check_totals = totals
        if simple_totals is not None:
            if not measures:
                check_totals = None
            else:
                check_totals = ([
                    tuple(rows[:row_tot_item.level])+tuple(columns)
                    for row_tot_item in simple_totals.rows
                ] if rows else [])+([
                    tuple(columns[:col_tot_item.level])+tuple(rows)
                    for col_tot_item in simple_totals.columns
                ] if columns else [])+([
                    tuple(rows[:row_tot_item.level])+tuple(columns[:col_tot_item.level])
                    for row_tot_item in simple_totals.rows
                    for col_tot_item in simple_totals.columns
                ] if rows and columns else [])

    pivot_resp = get_pivot_response(
        dataset=dataset, data_api=data_api, columns=columns, rows=rows, measures=measures,
        measures_sorting_settings=measures_sorting_settings,
        annotations=annotations, title_mapping=title_mapping,
        totals=totals, simple_totals=simple_totals, with_totals=with_totals,
        order_fields=order_fields, filters=filters,
        autofill_legend=autofill_legend,
    )

    assert pivot_resp.status_code == HTTPStatus.OK, f'Expected HTTP status 200, got {pivot_resp.status_code}'

    expected_pivot_legend_tuples: list[Tuple[str, PivotRole]]
    if custom_pivot_legend_check is not None:
        # make a copy that can be appended to
        expected_pivot_legend_tuples = [item for item in custom_pivot_legend_check]
    else:
        expected_pivot_legend_tuples = [
            *[(norm_title(title), PivotRole.pivot_column) for title in columns],
            *[(norm_title(title), PivotRole.pivot_row) for title in rows],
            *[(norm_title(title), PivotRole.pivot_measure) for title in measures],
            *[(norm_title(title), PivotRole.pivot_annotation) for title in (annotations or ())],
            (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
        ]

    actual_pivot_legend_tuples = [
        (item.title, item.role_spec.role)
        for item in pivot_resp.data['pivot']['structure']
    ]
    assert actual_pivot_legend_tuples == expected_pivot_legend_tuples

    legend, liid_map = _get_regular_legend_for_pivot(
        dataset=dataset, columns=columns, rows=rows, measures=measures,
        annotations=annotations, totals=check_totals,
    )
    legend_for_result = [item for item in legend if item.ref.type != QueryItemRefType.measure_name]
    result_resp = data_api.get_result(dataset=dataset, fields=legend_for_result, filters=filters, fail_ok=True)
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json

    result_mapper_measure_liids = frozenset(chain.from_iterable(
        liid_map[title] for title in chain(measures, annotations or ())
    ))

    result_data_abstraction = ResultDataAbstraction.from_response(result_resp)
    result_data_mapper: DataCellMapper1D = result_data_abstraction.get_1d_mapper(
        measure_liids=result_mapper_measure_liids)
    pivot_data_abstraction = PivotDataAbstraction.from_response(pivot_resp)
    pivot_data_mapper: DataCellMapper1D = pivot_data_abstraction.get_1d_mapper()

    def _is_total(dimension: Optional[list], totals_liids: set[int]):
        return dimension is not None and any(value[0][1] in totals_liids for value in dimension)

    totals_liids = {item.legend_item_id for item in pivot_resp.data['fields'] if item.role_spec.role.value == 'template'}
    for column in pivot_resp.data['pivot_data']['columns_with_info']:
        role = column['header_info'].role_spec.role.value
        expected_role = 'total' if _is_total(column['cells'], totals_liids) else 'data'
        assert role == expected_role, f'Column {column["cells"]} is {role}, but it expected to be {expected_role}'
    for row in pivot_resp.data['pivot_data']['rows']:
        role = row['header_with_info']['header_info'].role_spec.role.value
        expected_role = 'total' if _is_total(row['header'], totals_liids) else 'data'
        assert role == expected_role, f'Row {row["header"]} is {role}, but it expected to be {expected_role}'

    # workaround for missing annotations for totals
    # remove them from the result mapper
    if annotations:

        def apply_anno_tag(dims: DataCellTuple, data_item: DataItem) -> AbstractSet[DataItemTag]:
            if data_item.cell.title in anno_only:
                return data_item.meta.tags | {DataItemTag.annotation}
            return data_item.meta.tags

        anno_only = set(annotations) - set(measures)  # annotations that are not present as regular measures
        if anno_only:
            result_data_mapper = result_data_mapper.apply_tagger(apply_anno_tag)
            result_data_mapper = result_data_mapper.apply_filter(
                exclude_tag_combos={frozenset({DataItemTag.total, DataItemTag.annotation})},
            )

    SimpleMapper1DComparator().assert_equal(pivot_data_mapper, result_data_mapper)

    if expected_notifications is not None:
        actual_notifications = [
            NotificationType(notification['locator'])
            for notification in pivot_resp.data['notifications']
        ]
        assert actual_notifications == expected_notifications

    pivot_data = pivot_resp.data['pivot_data']
    pivot_rows = pivot_data['rows']

    # Check columns
    if min_col_cnt is not None:
        assert len(pivot_data['columns']) >= min_col_cnt
    if max_col_cnt is not None:
        assert len(pivot_data['columns']) <= max_col_cnt

    # Check row dimension headers
    actual_row_dim_headers = [header[0][0] for header in pivot_data['row_dimension_headers']]
    expected_row_dim_headers = [
        item.title for item in pivot_resp.data['pivot']['structure']
        if item.role_spec.role == PivotRole.pivot_row
    ]
    assert actual_row_dim_headers == expected_row_dim_headers

    # Check row headers
    if min_row_cnt is not None:
        assert len(pivot_rows) >= min_row_cnt
    if max_row_cnt is not None:
        assert len(pivot_rows) <= max_row_cnt

    # Check measure values
    pivot_value_cnt = pivot_data_mapper.get_value_count()
    if min_value_cnt is not None:
        assert pivot_value_cnt >= min_value_cnt
    if max_value_cnt is not None:
        assert pivot_value_cnt <= max_value_cnt

    block_meta_list = pivot_resp.json['blocks']
    if not check_totals:
        assert len(block_meta_list) == 1
    block_meta = block_meta_list[0]
    assert block_meta['block_id'] == 0
    assert block_meta['query'] is not None

    return pivot_data_abstraction


def get_all_measure_cells(pivot_rows: list[dict]) -> list[list[list]]:
    return [
        cell  # Each cell is a list of pairs
        for row in pivot_rows
        for cell in row['values']
        if cell is not None
    ]
