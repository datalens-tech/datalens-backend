from http import HTTPStatus
from typing import Optional

from bi_constants.internal_constants import MEASURE_NAME_TITLE
from bi_constants.enums import OrderDirection

from bi_api_client.dsmaker.primitives import PivotPagination
from bi_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from bi_api_client.dsmaker.shortcuts.result_data import get_regular_result_data
from bi_api_client.dsmaker.pivot_utils import check_pivot_response, get_pivot_response
from bi_api_lib.pivot.primitives import PivotMeasureSorting, PivotMeasureSortingSettings, PivotHeaderValue


def test_basic_pivot(api_v1, data_api_v2_test_mutation_cache, dataset_id):
    data_api_v2, *_ = data_api_v2_test_mutation_cache
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
    })

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category'], rows=['Order Date'], measures=['Sales Sum'],
        min_col_cnt=3, min_row_cnt=100, min_value_cnt=100,
    )


def test_pivot_multiple_measures(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Sales])',
    })

    pivot_abs = check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category', 'Sub-Category'],
        rows=['Order Date', MEASURE_NAME_TITLE, 'City'],
        measures=['Sales Sum', 'Profit Sum'],
        measures_sorting_settings=[
            PivotMeasureSorting(
                row=PivotMeasureSortingSettings(
                    header_values=[
                        PivotHeaderValue(value='2014-01-04'),
                        PivotHeaderValue(value='Sales Sum'),
                        PivotHeaderValue(value='Naperville'),
                    ],
                )
            ),
            None,
        ],
        min_col_cnt=10, min_row_cnt=100, min_value_cnt=100,
    )

    sorting_row_idx = None
    for row in pivot_abs.iter_rows():
        if row.get_compound_header() == ('2014-01-04', 'Sales Sum', 'Naperville'):
            sorting_row_idx = row.row_idx
            break
    assert sorting_row_idx is not None

    def _get_value(value: Optional[tuple]) -> float:
        if value is None:
            return float('-inf')
        return float(value[0][0])

    row_values = list(map(_get_value, pivot_abs.resp_data['pivot_data']['rows'][sorting_row_idx]['values']))
    assert sorted(row_values) == row_values


def test_pivot_with_order_by(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Sales])',
    })

    data_by_field = get_regular_result_data(
        ds, data_api_v2, field_names=[
            'Category', 'Sub-Category', 'Order Date', 'City', 'Sales Sum', 'Profit Sum',
        ]
    )

    pivot_resp = get_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category', 'Sub-Category'],
        rows=['Order Date', 'City', MEASURE_NAME_TITLE],
        measures=['Sales Sum', 'Profit Sum'],
        order_fields={'Category': OrderDirection.desc, 'Order Date': OrderDirection.desc}
    )
    assert pivot_resp.status_code == HTTPStatus.OK
    result_data = pivot_resp.data

    # Check first column dimension
    category_values = []
    for col in result_data['pivot_data']['columns']:
        value = col[0][0][0]
        if value not in category_values:
            category_values.append(value)
    assert category_values == sorted(set(data_by_field['Category']), reverse=True)

    # Check first row dimension
    pivot_rows = result_data['pivot_data']['rows']
    date_values = []
    for row in pivot_rows:
        value = row['header'][0][0][0]
        if value not in date_values:
            date_values.append(value)
    assert date_values == sorted(set(data_by_field['Order Date']), reverse=True)


def test_pivot_with_pagination(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Sales])',
    })

    def get_pivot(pivot_pagination: Optional[PivotPagination] = None):
        pivot_resp = get_pivot_response(
            dataset=ds, data_api=data_api_v2,
            columns=['Category', 'Sub-Category'],
            rows=['Order Date', 'City', MEASURE_NAME_TITLE],
            measures=['Sales Sum', 'Profit Sum'],
            pivot_pagination=pivot_pagination,
        )
        assert pivot_resp.status_code == HTTPStatus.OK, pivot_resp.response_errors
        return pivot_resp.data

    # Save unpaginated table:
    result_data = get_pivot()
    initial_columns = result_data['pivot_data']['columns']
    initial_rows = result_data['pivot_data']['rows']

    # Get paginated table and compare
    result_data = get_pivot(pivot_pagination=PivotPagination(offset_rows=1, limit_rows=2))
    paginated_columns = result_data['pivot_data']['columns']
    paginated_rows = result_data['pivot_data']['rows']
    assert paginated_columns == initial_columns
    assert paginated_rows == initial_rows[1:3]

    # Pseudo-pagination
    result_data = get_pivot(pivot_pagination=PivotPagination(offset_rows=0, limit_rows=None))
    paginated_columns = result_data['pivot_data']['columns']
    paginated_rows = result_data['pivot_data']['rows']
    assert paginated_columns == initial_columns
    assert paginated_rows == initial_rows


def test_pivot_with_remapped_titles(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
    })

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category'], rows=['Order Date'], measures=['Sales Sum'],
        title_mapping={
            'Category': 'My Dimension 1',
            'Order Date': 'My Dimension 2',
            'Sales Sum': 'Measure',
        },
        min_col_cnt=3, min_row_cnt=100, min_value_cnt=100,
    )
