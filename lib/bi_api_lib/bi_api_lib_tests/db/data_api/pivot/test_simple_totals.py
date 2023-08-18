from bi_constants.internal_constants import MEASURE_NAME_TITLE, DIMENSION_NAME_TITLE
from bi_constants.enums import PivotRole

from bi_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from bi_api_client.dsmaker.pivot_utils import check_pivot_response
from bi_api_client.dsmaker.primitives import PivotTotals


def test_main_totals(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
    })

    pivot_abs = check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category'], rows=['Region'], measures=['Sales Sum'],
        simple_totals=PivotTotals(
            rows=[PivotTotals.item(level=0)],
            columns=[PivotTotals.item(level=0)],
        ),
    )
    col_titles = pivot_abs.get_flat_column_headers()
    row_titles = pivot_abs.get_flat_row_headers()
    assert col_titles == ['Furniture', 'Office Supplies', 'Technology', '']
    assert row_titles == ['Central', 'East', 'South', 'West', '']


def test_with_totals_flag(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
    })

    pivot_abs = check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category'], rows=['Region'], measures=['Sales Sum'],
        with_totals=True,
    )
    col_titles = pivot_abs.get_flat_column_headers()
    row_titles = pivot_abs.get_flat_row_headers()
    assert col_titles == ['Furniture', 'Office Supplies', 'Technology', '']
    assert row_titles == ['Central', 'East', 'South', 'West', '']


def test_corner_case_totals(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
    })

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category'], rows=[], measures=['Sales Sum'],
        simple_totals=PivotTotals(
            rows=[],
            columns=[PivotTotals.item(level=0)],
        ),
        custom_pivot_legend_check=[
            ('Category', PivotRole.pivot_column),
            ('Sales Sum', PivotRole.pivot_measure),
            (MEASURE_NAME_TITLE, PivotRole.pivot_row),
            (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
        ],
    )

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=[], rows=['Category'], measures=['Sales Sum'],
        simple_totals=PivotTotals(
            rows=[PivotTotals.item(level=0)],
            columns=[],
        ),
        custom_pivot_legend_check=[
            ('Category', PivotRole.pivot_row),
            ('Sales Sum', PivotRole.pivot_measure),
            (MEASURE_NAME_TITLE, PivotRole.pivot_column),
            (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
        ],
    )

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category'], rows=['Region'], measures=[],
        simple_totals=PivotTotals(
            rows=[PivotTotals.item(level=0)],
            columns=[PivotTotals.item(level=0)],
        ),
    )


def test_multi_measure_corner_case_totals_flag(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Profit])',
    })

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category'], rows=[], measures=['Sales Sum', 'Profit Sum'],
        with_totals=True,
        custom_pivot_legend_check=[
            ('Category', PivotRole.pivot_column),
            ('Sales Sum', PivotRole.pivot_measure),
            ('Profit Sum', PivotRole.pivot_measure),
            (MEASURE_NAME_TITLE, PivotRole.pivot_row),
            (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
        ],
    )

    check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=[], rows=['Category'], measures=['Sales Sum', 'Profit Sum'],
        simple_totals=PivotTotals(
            rows=[PivotTotals.item(level=0)],
            columns=[],
        ),
        custom_pivot_legend_check=[
            ('Category', PivotRole.pivot_row),
            ('Sales Sum', PivotRole.pivot_measure),
            ('Profit Sum', PivotRole.pivot_measure),
            (MEASURE_NAME_TITLE, PivotRole.pivot_column),
            (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
        ],
    )


def test_main_totals_with_annotation(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Sales])',
    })

    pivot_abs = check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category'], rows=['Region'], measures=['Sales Sum'],
        annotations=['Profit Sum'],
        simple_totals=PivotTotals(
            rows=[PivotTotals.item(level=0)],
            columns=[PivotTotals.item(level=0)],
        ),
    )
    col_titles = pivot_abs.get_flat_column_headers()
    row_titles = pivot_abs.get_flat_row_headers()
    assert col_titles == ['Furniture', 'Office Supplies', 'Technology', '']
    assert row_titles == ['Central', 'East', 'South', 'West', '']


def test_main_totals_with_multiple_measures(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Sales])',
    })

    pivot_abs = check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category'], rows=['Region', MEASURE_NAME_TITLE],
        measures=['Sales Sum', 'Profit Sum'],
        simple_totals=PivotTotals(
            rows=[PivotTotals.item(level=0)],
            columns=[PivotTotals.item(level=0)],
        ),
    )
    col_titles = pivot_abs.get_flat_column_headers()
    row_compound_titles = pivot_abs.get_compound_row_headers()
    assert col_titles == ['Furniture', 'Office Supplies', 'Technology', '']
    assert row_compound_titles == [
        ('Central', 'Sales Sum'),
        ('Central', 'Profit Sum'),
        ('East', 'Sales Sum'),
        ('East', 'Profit Sum'),
        ('South', 'Sales Sum'),
        ('South', 'Profit Sum'),
        ('West', 'Sales Sum'),
        ('West', 'Profit Sum'),
        ('', 'Sales Sum'),
        ('', 'Profit Sum'),
    ]


def test_subtotals_with_multiple_measures(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Sales])',
    })

    pivot_abs = check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category'], rows=[MEASURE_NAME_TITLE, 'Region'],
        measures=['Sales Sum', 'Profit Sum'],
        simple_totals=PivotTotals(
            rows=[PivotTotals.item(level=1)],
            columns=[PivotTotals.item(level=0)],
        ),
    )
    col_titles = pivot_abs.get_flat_column_headers()
    row_compound_titles = pivot_abs.get_compound_row_headers()
    assert col_titles == ['Furniture', 'Office Supplies', 'Technology', '']
    assert row_compound_titles == [
        ('Sales Sum', 'Central'),
        ('Sales Sum', 'East'),
        ('Sales Sum', 'South'),
        ('Sales Sum', 'West'),
        ('Sales Sum', ''),
        ('Profit Sum', 'Central'),
        ('Profit Sum', 'East'),
        ('Profit Sum', 'South'),
        ('Profit Sum', 'West'),
        ('Profit Sum', '')
    ]


def test_main_totals_with_only_mnames_one_one_side(api_v1, data_api_v2, dataset_id):
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Sales])',
    })

    # 1. Only Measure Names in rows
    pivot_abs = check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=['Category'], rows=[MEASURE_NAME_TITLE],
        measures=['Sales Sum', 'Profit Sum'],
        with_totals=True,
        check_totals=[()],  # override the autogenerated value because this is a corner case
    )
    col_titles = pivot_abs.get_flat_column_headers()
    row_titles = pivot_abs.get_flat_row_headers()
    assert col_titles == ['Furniture', 'Office Supplies', 'Technology', '']
    assert row_titles == ['Sales Sum', 'Profit Sum']

    # 2. Only Measure Names in columns
    pivot_abs = check_pivot_response(
        dataset=ds, data_api=data_api_v2,
        columns=[MEASURE_NAME_TITLE], rows=['Category'],
        measures=['Sales Sum', 'Profit Sum'],
        with_totals=True,
        check_totals=[()],  # override the autogenerated value because this is a corner case
    )
    col_titles = pivot_abs.get_flat_column_headers()
    row_titles = pivot_abs.get_flat_row_headers()
    assert col_titles == ['Sales Sum', 'Profit Sum']
    assert row_titles == ['Furniture', 'Office Supplies', 'Technology', '']
