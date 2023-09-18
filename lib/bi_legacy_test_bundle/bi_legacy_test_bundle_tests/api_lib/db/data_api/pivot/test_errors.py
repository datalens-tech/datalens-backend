from http import HTTPStatus

from dl_constants.internal_constants import MEASURE_NAME_TITLE
from dl_constants.enums import PivotRole, FieldRole

from dl_api_client.dsmaker.primitives import Dataset, PivotPagination, PivotTotals
from dl_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from dl_api_client.dsmaker.pivot_utils import get_pivot_response
from dl_api_lib.pivot.primitives import PivotMeasureSorting, PivotMeasureSortingSettings, PivotHeaderValue


def test_multiple_measures_without_measure_name(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Sales])',
    })

    pivot_resp = get_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category'], rows=['Order Date'],
        measures=['Sales Sum', 'Profit Sum'],
    )
    assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
    assert pivot_resp.bi_status_code == 'ERR.DS_API.PIVOT.MEASURE_NAME.REQUIRED'


def test_no_measures_with_measure_name(api_v1, data_api_v2, dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset

    pivot_resp = get_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category'], rows=['Order Date', MEASURE_NAME_TITLE],
        measures=[],
    )
    assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
    assert pivot_resp.bi_status_code == 'ERR.DS_API.PIVOT.MEASURE_NAME.FORBIDDEN'


def test_multiple_measures_with_double_measure_name(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Sales])',
    })

    pivot_resp = get_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category', MEASURE_NAME_TITLE],
        rows=['Order Date', MEASURE_NAME_TITLE],
        measures=['Sales Sum', 'Profit Sum'],
    )
    assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
    assert pivot_resp.bi_status_code == 'ERR.DS_API.PIVOT.MEASURE_NAME.DUPLICATE'


def test_pagination_range(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
    })

    def check_pagination_error(pivot_pagination: PivotPagination) -> None:
        pivot_resp = get_pivot_response(
            dataset=ds, data_api=data_api_v2,
            columns=['Category'],
            rows=['Order Date'],
            measures=['Sales Sum'],
            pivot_pagination=pivot_pagination,
        )
        assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST

    check_pagination_error(PivotPagination(limit_rows=-9, offset_rows=8))
    check_pagination_error(PivotPagination(limit_rows=9, offset_rows=-8))
    check_pagination_error(PivotPagination(limit_rows=0, offset_rows=1))


def test_measure_as_dimension(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Profit])',
    })

    pivot_resp = get_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category'], rows=['Sales Sum'],
        measures=['Profit Sum'],
    )
    assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
    assert pivot_resp.bi_status_code == 'ERR.DS_API.PIVOT.LEGEND.INVALID_ROLE'


def test_dimension_as_measure(api_v1, data_api_v2, dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset

    pivot_resp = get_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category'], rows=['City'],
        measures=['Order Date'],
    )
    assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
    assert pivot_resp.bi_status_code == 'ERR.DS_API.PIVOT.LEGEND.INVALID_ROLE'


def test_uneven_data_columns(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
    })

    pivot_resp = data_api_v2.get_pivot(
        dataset=ds,
        fields=[
            ds.find_field(title='Category').as_req_legend_item(legend_item_id=0, block_id=0),
            ds.find_field(title='Sales Sum').as_req_legend_item(legend_item_id=1, block_id=0),
            ds.find_field(title='Order Date').as_req_legend_item(legend_item_id=2, block_id=1),
            ds.find_field(title='Sales Sum').as_req_legend_item(legend_item_id=3, role=FieldRole.total, block_id=1),
        ],
        pivot_structure=[
            ds.make_req_pivot_item(role=PivotRole.pivot_row, legend_item_ids=[0]),
            ds.make_req_pivot_item(role=PivotRole.pivot_column, legend_item_ids=[2]),
            ds.make_req_pivot_item(role=PivotRole.pivot_measure, legend_item_ids=[1, 3]),
        ],
        fail_ok=True,
    )
    assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
    assert pivot_resp.bi_status_code == 'ERR.DS_API.PIVOT.UNEVEN_DATA_COLUMNS'


def test_wrong_column_in_measure_sorting(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
    })

    pivot_resp = get_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category'], rows=['Order Date'],
        measures=['Sales Sum'],
        measures_sorting_settings=[PivotMeasureSorting(
            column=PivotMeasureSortingSettings(header_values=[PivotHeaderValue(value='Not found')])
        )],
    )
    assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
    assert pivot_resp.bi_status_code == 'ERR.DS_API.PIVOT.SORTING.ROW_OR_COLUMN_NOT_FOUND'


def test_wrong_role_spec_in_measure_sorting(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
    })

    pivot_resp = get_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category'], rows=['Order Date'],
        measures=['Sales Sum'], totals=[()],
        measures_sorting_settings=[PivotMeasureSorting(
            column=PivotMeasureSortingSettings(header_values=[PivotHeaderValue(value='')])  # sort by data, not totals!
        )],
    )
    assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
    assert pivot_resp.bi_status_code == 'ERR.DS_API.PIVOT.SORTING.ROW_OR_COLUMN_NOT_FOUND'


def test_measure_sorting_multiple_sort_on_the_same_axis(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Sales])',
    })
    sorting_settings = PivotMeasureSorting(
        column=PivotMeasureSortingSettings(
            header_values=[PivotHeaderValue(value='Furniture'), PivotHeaderValue(value='Sales Sum')]
        )
    )

    pivot_resp = get_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category', MEASURE_NAME_TITLE], rows=['Order Date'],
        measures=['Sales Sum', 'Profit Sum'],
        measures_sorting_settings=[sorting_settings, sorting_settings],
    )
    assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
    assert pivot_resp.bi_status_code == 'ERR.DS_API.PIVOT.SORTING.MULTIPLE_COLUMNS_OR_ROWS'


def test_measure_sorting_by_column_with_multiple_measures_in_rows(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Sales])',
    })
    sorting_settings = PivotMeasureSorting(
        column=PivotMeasureSortingSettings(
            header_values=[PivotHeaderValue(value='Furniture'), PivotHeaderValue(value='Sales Sum')]
        )
    )

    pivot_resp = get_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category'], rows=['Order Date', MEASURE_NAME_TITLE],
        measures=['Sales Sum', 'Profit Sum'],
        measures_sorting_settings=[sorting_settings, None],
    )
    assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
    assert pivot_resp.bi_status_code == 'ERR.DS_API.PIVOT.SORTING.AGAINST_MULTIPLE_MEASURES'


def test_measure_sorting_with_subtotals(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Sales])',
    })

    pivot_resp = get_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=[MEASURE_NAME_TITLE], rows=['Category', 'Order Date'],
        measures=['Sales Sum', 'Profit Sum'],
        measures_sorting_settings=[PivotMeasureSorting(
            column=PivotMeasureSortingSettings(header_values=[PivotHeaderValue(value='Sales Sum')])
        ), None], simple_totals=PivotTotals(
            rows=[PivotTotals.item(level=1)],
        ),
    )
    assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
    assert pivot_resp.bi_status_code == 'ERR.DS_API.PIVOT.SORTING.SUBTOTALS_ARE_NOT_ALLOWED'
