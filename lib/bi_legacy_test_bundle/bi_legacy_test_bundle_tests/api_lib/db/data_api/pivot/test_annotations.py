from http import HTTPStatus

import pytest

from dl_constants.internal_constants import MEASURE_NAME_TITLE, DIMENSION_NAME_TITLE
from dl_constants.enums import PivotRole

from dl_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from dl_api_client.dsmaker.shortcuts.result_data import get_regular_result_data
from dl_api_client.dsmaker.pivot_utils import get_all_measure_cells, check_pivot_response


def test_pivot_multiple_measures_with_annotation(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Profit])',
        'Order Count': 'COUNT([Sales])',
    })

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category', 'Sub-Category'],
        rows=['Order Date', 'City', MEASURE_NAME_TITLE],
        measures=['Sales Sum', 'Profit Sum'],
        annotations=['Order Count'],
        min_col_cnt=10, min_row_cnt=100, min_value_cnt=100,
    )


def test_pivot_multiple_measures_with_targeted_annotation(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Profit])',
        'Order Count': 'COUNT([Sales])',
    })

    data_by_field = get_regular_result_data(
        ds, data_api_v2, field_names=[
            'Category', 'Sub-Category', 'Order Date', 'City',
            'Sales Sum', 'Profit Sum', 'Order Count',
        ]
    )
    category_leg_id = 0
    subcategory_leg_id = 1
    orderdate_leg_id = 2
    city_leg_id = 3
    mname_leg_id = 4
    profit_leg_id = 5
    sales_leg_id = 6
    anno_leg_id = 7

    result_resp = data_api_v2.get_pivot(
        dataset=ds,
        fields=[
            ds.find_field(title='Category').as_req_legend_item(legend_item_id=category_leg_id),
            ds.find_field(title='Sub-Category').as_req_legend_item(legend_item_id=subcategory_leg_id),
            ds.find_field(title='Order Date').as_req_legend_item(legend_item_id=orderdate_leg_id),
            ds.find_field(title='City').as_req_legend_item(legend_item_id=city_leg_id),
            ds.measure_name_as_req_legend_item(legend_item_id=mname_leg_id),
            ds.find_field(title='Sales Sum').as_req_legend_item(legend_item_id=sales_leg_id),
            ds.find_field(title='Profit Sum').as_req_legend_item(legend_item_id=profit_leg_id),
            ds.find_field(title='Order Count').as_req_legend_item(legend_item_id=anno_leg_id),
        ],
        pivot_structure=[
            ds.make_req_pivot_item(role=PivotRole.pivot_column, legend_item_ids=[category_leg_id]),
            ds.make_req_pivot_item(role=PivotRole.pivot_column, legend_item_ids=[subcategory_leg_id]),
            ds.make_req_pivot_item(role=PivotRole.pivot_row, legend_item_ids=[orderdate_leg_id]),
            ds.make_req_pivot_item(role=PivotRole.pivot_row, legend_item_ids=[city_leg_id]),
            ds.make_req_pivot_item(role=PivotRole.pivot_row, legend_item_ids=[mname_leg_id]),
            ds.make_req_pivot_item(role=PivotRole.pivot_measure, legend_item_ids=[sales_leg_id]),
            ds.make_req_pivot_item(role=PivotRole.pivot_measure, legend_item_ids=[profit_leg_id]),
            ds.make_req_pivot_item(
                role=PivotRole.pivot_annotation, legend_item_ids=[anno_leg_id],
                annotation_type='color', target_legend_item_ids=[profit_leg_id],
            ),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK
    result_data = result_resp.data

    # Check legend
    legend_tuples = [(item.title, item.role_spec.role) for item in result_data['pivot']['structure']]
    assert legend_tuples == [
        ('Category', PivotRole.pivot_column),
        ('Sub-Category', PivotRole.pivot_column),
        ('Order Date', PivotRole.pivot_row),
        ('City', PivotRole.pivot_row),
        (MEASURE_NAME_TITLE, PivotRole.pivot_row),
        ('Sales Sum', PivotRole.pivot_measure),
        ('Profit Sum', PivotRole.pivot_measure),
        ('Order Count', PivotRole.pivot_annotation),
        (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
    ]

    # Check measure values
    pivot_rows = result_data['pivot_data']['rows']
    # Check annotation values
    all_measure_cells = get_all_measure_cells(pivot_rows)
    sales_annotation_values = sorted({
        float(cell[1][0]) for cell in all_measure_cells
        if cell[0][1] == sales_leg_id and len(cell) > 1
    })
    profit_annotation_values = sorted({
        float(cell[1][0]) for cell in all_measure_cells
        if cell[0][1] == profit_leg_id and len(cell) > 1
    })
    original_annotation_values = sorted({float(val) for val in data_by_field['Order Count']})
    assert sales_annotation_values == []
    assert profit_annotation_values == pytest.approx(original_annotation_values)


def test_pivot_multi_measures_with_annotation_same_as_one_measure(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Count': 'COUNT()',
    })

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category', 'Sub-Category'],
        rows=['Order Date', 'City', MEASURE_NAME_TITLE],
        measures=['Sales Sum', 'Count'],
        annotations=['Sales Sum'],
        min_col_cnt=10, min_row_cnt=100, min_value_cnt=100,
    )


def test_pivot_with_multiple_annotations(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Profit])',
        'Order Count': 'COUNT([Sales])',
    })

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category'],
        rows=['Order Date'],
        measures=['Sales Sum'],
        annotations=['Profit Sum', 'Order Count'],
    )


def test_pivot_with_annotation_by_dimension(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Order Count': 'COUNT([Sales])',
    })

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category'],
        rows=['Order Date'],
        measures=['Sales Sum'],
        annotations=['Order Date'],
    )
