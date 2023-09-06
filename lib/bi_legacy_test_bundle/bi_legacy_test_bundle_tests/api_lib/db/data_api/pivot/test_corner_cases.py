import json
from http import HTTPStatus
from typing import Any


from bi_constants.internal_constants import MEASURE_NAME_TITLE, DIMENSION_NAME_TITLE
from bi_constants.enums import PivotRole, OrderDirection

from bi_api_client.dsmaker.primitives import Dataset
from bi_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from bi_api_client.dsmaker.data_abstraction.pivot import PivotDataAbstraction
from bi_api_client.dsmaker.shortcuts.result_data import get_regular_result_data
from bi_api_client.dsmaker.pivot_utils import get_all_measure_cells, check_pivot_response


def test_pivot_with_markup(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Category MU': 'BOLD([Category])',
        'Order Date MU': 'ITALIC(STR([Order Date]))',
        'Measure MU': 'BOLD(STR(SUM([Sales]))) + " - " + ITALIC(STR(SUM([Profit])))',
    })

    data_by_field = get_regular_result_data(
        ds, data_api_v2, field_names=['Category MU', 'Order Date MU', 'Measure MU'])

    category_liid = 0
    orderdate_liid = 1
    measure_liid = 2

    result_resp = data_api_v2.get_pivot(
        dataset=ds,
        fields=[
            ds.find_field(title='Category MU').as_req_legend_item(legend_item_id=category_liid),
            ds.find_field(title='Order Date MU').as_req_legend_item(legend_item_id=orderdate_liid),
            ds.find_field(title='Measure MU').as_req_legend_item(legend_item_id=measure_liid),
        ],
        pivot_structure=[
            ds.make_req_pivot_item(role=PivotRole.pivot_column, legend_item_ids=[category_liid]),
            ds.make_req_pivot_item(role=PivotRole.pivot_row, legend_item_ids=[orderdate_liid]),
            ds.make_req_pivot_item(role=PivotRole.pivot_measure, legend_item_ids=[measure_liid]),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK
    result_data = result_resp.data

    # Check legend
    legend_tuples = [(item.title, item.role_spec.role) for item in result_data['pivot']['structure']]
    assert legend_tuples == [
        ('Category MU', PivotRole.pivot_column),
        ('Order Date MU', PivotRole.pivot_row),
        ('Measure MU', PivotRole.pivot_measure),
        (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
    ]

    def dumps(value: Any) -> str:
        return json.dumps(value, sort_keys=True)

    # Check columns
    category_values = {dumps(col[0][0][0]) for col in result_data['pivot_data']['columns']}
    assert category_values == {
        dumps({'type': 'bold', 'content': {'type': 'text', 'content': 'Office Supplies'}}),
        dumps({'type': 'bold', 'content': {'type': 'text', 'content': 'Furniture'}}),
        dumps({'type': 'bold', 'content': {'type': 'text', 'content': 'Technology'}}),
    }

    # Check row headers
    pivot_rows = result_data['pivot_data']['rows']
    date_values = {dumps(row['header'][0][0][0]) for row in pivot_rows}
    assert date_values.issuperset({
        dumps({'type': 'italics', 'content': {'type': 'text', 'content': '2014-01-03'}}),
        dumps({'type': 'italics', 'content': {'type': 'text', 'content': '2014-01-06'}}),
        dumps({'type': 'italics', 'content': {'type': 'text', 'content': '2014-01-09'}}),
    })

    # Check measure values
    assert len(pivot_rows) > 100
    all_measure_cells = get_all_measure_cells(pivot_rows)
    measure_values = sorted({dumps(cell[0][0]) for cell in all_measure_cells})
    original_measure_values = sorted({dumps(val) for val in data_by_field['Measure MU']})
    assert measure_values == original_measure_values


def test_pivot_no_measures(api_v1, data_api_v2, dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category', 'Sub-Category'],
        rows=['Order Date', 'City'],
        measures=[],
        min_col_cnt=10, min_row_cnt=100, max_value_cnt=0,
    )


def test_pivot_no_dimensions_multiple_measures(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Sales])',
    })

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=[], rows=[],
        measures=['Sales Sum', 'Profit Sum'],
        min_col_cnt=2, max_col_cnt=2,
        min_row_cnt=1, max_row_cnt=1,
        min_value_cnt=2, max_value_cnt=2,
        custom_pivot_legend_check=[
            ('Sales Sum', PivotRole.pivot_measure),
            ('Profit Sum', PivotRole.pivot_measure),
            (MEASURE_NAME_TITLE, PivotRole.pivot_column),
            (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
        ],
    )


def test_pivot_only_row_dimensions_one_measure(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
    })

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=[],
        rows=['Category', 'Order Date'],
        measures=['Sales Sum'],
        min_col_cnt=1, max_col_cnt=1,
        custom_pivot_legend_check=[
            ('Category', PivotRole.pivot_row),
            ('Order Date', PivotRole.pivot_row),
            ('Sales Sum', PivotRole.pivot_measure),
            (MEASURE_NAME_TITLE, PivotRole.pivot_column),
            (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
        ],
    )


def test_pivot_only_column_dimensions_multiple_measures_no_mnames(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Sales])',
    })

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category', 'Order Date'],
        rows=[],
        measures=['Sales Sum', 'Profit Sum'],
        min_row_cnt=2, max_row_cnt=2,
        custom_pivot_legend_check=[
            ('Category', PivotRole.pivot_column),
            ('Order Date', PivotRole.pivot_column),
            ('Sales Sum', PivotRole.pivot_measure),
            ('Profit Sum', PivotRole.pivot_measure),
            (MEASURE_NAME_TITLE, PivotRole.pivot_row),
            (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
        ],
    )


def test_pivot_only_column_dimensions_multiple_measures_with_mnames(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Sales])',
    })

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category', 'Order Date', MEASURE_NAME_TITLE],
        rows=[],
        measures=['Sales Sum', 'Profit Sum'],
        min_row_cnt=1, max_row_cnt=1,
    )


def test_single_measure_with_duplicate_measure_name(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
    })

    category_liid = 0
    orderdate_liid = 1
    sales_liid = 2
    mname_liid = 3

    def get_pivot(duplicate_measure_name: bool) -> dict:
        result_resp = data_api_v2.get_pivot(
            dataset=ds,
            fields=[
                ds.find_field(title='Category').as_req_legend_item(legend_item_id=category_liid),
                ds.find_field(title='Order Date').as_req_legend_item(legend_item_id=orderdate_liid),
                ds.find_field(title='Sales Sum').as_req_legend_item(legend_item_id=sales_liid),
                ds.measure_name_as_req_legend_item(legend_item_id=mname_liid),
            ],
            pivot_structure=[
                ds.make_req_pivot_item(role=PivotRole.pivot_column, legend_item_ids=[category_liid]),
                ds.make_req_pivot_item(role=PivotRole.pivot_column, legend_item_ids=[mname_liid]),
                ds.make_req_pivot_item(role=PivotRole.pivot_row, legend_item_ids=[orderdate_liid]),
                *(
                    (ds.make_req_pivot_item(role=PivotRole.pivot_row, legend_item_ids=[mname_liid]),)
                    if duplicate_measure_name else ()
                ),
                ds.make_req_pivot_item(role=PivotRole.pivot_measure, legend_item_ids=[sales_liid]),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK
        return result_resp.data

    single_result_data = get_pivot(duplicate_measure_name=False)
    double_result_data = get_pivot(duplicate_measure_name=True)

    # Check legend
    legend_tuples = [(item.title, item.role_spec.role) for item in double_result_data['pivot']['structure']]
    assert legend_tuples == [
        ('Category', PivotRole.pivot_column),
        ('Measure Names', PivotRole.pivot_column),
        ('Order Date', PivotRole.pivot_row),
        ('Measure Names', PivotRole.pivot_row),
        ('Sales Sum', PivotRole.pivot_measure),
        (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
    ]

    # Check column headers
    for col in double_result_data['pivot_data']['columns']:
        assert len(col) == 2

    # Check row headers
    for row in double_result_data['pivot_data']['rows']:
        assert len(row['header']) == 2

    assert len(double_result_data['pivot_data']['rows']) == len(single_result_data['pivot_data']['rows'])
    for single_row, double_row in zip(
            single_result_data['pivot_data']['rows'],
            double_result_data['pivot_data']['rows']
    ):
        assert len(single_row['values']) == len(double_row['values'])
        for single_cell, double_cell in zip(single_row['values'], double_row['values']):
            if single_cell is None or double_cell is None:
                assert single_cell == double_cell
            else:
                assert len(single_cell) == len(double_cell) == 1
                assert single_cell[0][0:2] == double_cell[0][0:2]  # exclude pivot_item_id (idx=2) from comparison


def test_pivot_only_row_dimensions_no_measures(api_v1, data_api_v2, dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=[],
        rows=['Category', 'Order Date'],
        measures=[],
        max_col_cnt=1,  # There will be 1 column without any headers or values
        min_row_cnt=100, max_value_cnt=0,
    )


def test_pivot_only_single_column_dimension_no_measures(api_v1, data_api_v2, dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Order Date'],
        rows=[],
        measures=[],
        max_row_cnt=1,  # There will be 1 row without any headers or values
        min_col_cnt=100, max_value_cnt=0,
    )


def test_pivot_duplicate_measures(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Sales])',
    })

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category', 'Sub-Category'],
        rows=['Order Date', 'City', MEASURE_NAME_TITLE],
        measures=['Sales Sum', 'Profit Sum', 'Sales Sum'],
        min_col_cnt=10, min_row_cnt=100, min_value_cnt=100,
    )


def test_pivot_empty_string_dimension_values(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Empty City': 'IF [City] = "New York" THEN "" ELSE [City] END',
    })

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Empty City'],
        rows=['Category'],
        measures=['Sales Sum'],
        order_fields={'Empty City': OrderDirection.desc},
        with_totals=True,
    )


def test_pivot_null_dimension_values(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Null City': 'IF [City] = "New York" THEN NULL ELSE [City] END',
    })

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Null City'],
        rows=['Category'],
        measures=['Sales Sum'],
        with_totals=True,
    )


def test_pivot_sorting_with_totals(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
    })

    def get_pivot(direction: OrderDirection) -> PivotDataAbstraction:
        return check_pivot_response(
            dataset=ds, data_api=data_api_v2,
            columns=['Category'],
            rows=['Region'],
            measures=['Sales Sum'],
            order_fields={'Category': direction},
            with_totals=True,
        )

    pivot_abs = get_pivot(OrderDirection.asc)
    col_titles = pivot_abs.get_flat_column_headers()
    assert col_titles == ['Furniture', 'Office Supplies', 'Technology', '']

    pivot_abs = get_pivot(OrderDirection.desc)
    col_titles = pivot_abs.get_flat_column_headers()
    assert col_titles == ['Technology', 'Office Supplies', 'Furniture', '']
