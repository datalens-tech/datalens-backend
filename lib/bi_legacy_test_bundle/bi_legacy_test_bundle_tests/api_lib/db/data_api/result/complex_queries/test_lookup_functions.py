from __future__ import annotations

import datetime
from http import HTTPStatus
from typing import Any, Callable, Optional

import pytest

from dl_constants.enums import WhereClauseOperation

from dl_api_client.dsmaker.primitives import Dataset, WhereClause
from dl_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset, create_basic_dataset
from dl_api_client.dsmaker.shortcuts.range_data import get_range_values
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows

from bi_connector_oracle.core.constants import CONNECTION_TYPE_ORACLE

from bi_legacy_test_bundle_tests.api_lib.utils import data_source_settings_from_table


def _read_date(s: str) -> datetime.date:
    try:
        return datetime.date.fromisoformat(s)
    except ValueError:
        return datetime.datetime.fromisoformat(s).date()


def _check_at_date_data(
        data_rows: list[list[Any]],
        date_idx: int, value_idx: int, ago_idx: int,
        ago_date_callable: Callable[[datetime.date], datetime.date],
        allow_missing_date_values: bool = False,
) -> None:
    assert len(data_rows) > 0
    value_by_date = {_read_date(row[date_idx]): row[value_idx] for row in data_rows}
    rows_checked = 0

    for row_idx, row in enumerate(data_rows):
        cur_date = _read_date(row[date_idx])
        ago_date = ago_date_callable(cur_date)
        expected_ago_value = value_by_date.get(ago_date)
        actual_ago_value = row[ago_idx]

        if expected_ago_value is None:
            if allow_missing_date_values:
                pass  # Do not check in this case
            else:
                assert actual_ago_value is None
        else:
            assert actual_ago_value == expected_ago_value

        rows_checked += 1

    # Make sure that rows were checked
    assert rows_checked > 5


def _check_ago_data(
        data_rows: list[list[Any]],
        date_idx: int, value_idx: int, ago_idx: int,
        day_offset: int,
        allow_missing_date_values: bool = False,
) -> None:

    def ago_date_callable(cur_date: datetime.date) -> datetime.date:  # noqa
        return cur_date - datetime.timedelta(days=day_offset)

    _check_at_date_data(
        data_rows=data_rows, date_idx=date_idx, value_idx=value_idx, ago_idx=ago_idx,
        ago_date_callable=ago_date_callable, allow_missing_date_values=allow_missing_date_values,
    )


def test_ago(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Sales Sum Yesterday': 'AGO([Sales Sum], [Order Date], "day", 1)',
    })

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Category'),
            ds.find_field(title='Order Date'),
            ds.find_field(title='Sales Sum'),
            ds.find_field(title='Sales Sum Yesterday'),
        ],
        order_by=[
            ds.find_field(title='Category'),
            ds.find_field(title='Order Date'),
        ],
        filters=[
            ds.find_field(title='Category').filter(op=WhereClauseOperation.EQ, values=['Office Supplies']),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)
    _check_ago_data(data_rows=data_rows, date_idx=1, value_idx=2, ago_idx=3, day_offset=1)


def test_ago_variants(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Ago 2': 'AGO([Sales Sum], [Order Date])',
        'Ago 3 unit': 'AGO([Sales Sum], [Order Date], "day")',
        'Ago 3 number': 'AGO([Sales Sum], [Order Date], 1)',
        'Ago 4': 'AGO([Sales Sum], [Order Date], "day", 1)',
    })

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Order Date'),
            ds.find_field(title='Sales Sum'),
            ds.find_field(title='Ago 2'),
            ds.find_field(title='Ago 3 unit'),
            ds.find_field(title='Ago 3 number'),
            ds.find_field(title='Ago 4'),
        ],
        order_by=[
            ds.find_field(title='Order Date'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)
    _check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=2, day_offset=1)
    _check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=3, day_offset=1)
    _check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=4, day_offset=1)
    _check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=5, day_offset=1)


def test_ago_any_db(any_db_saved_connection, api_v1, data_api_v2, any_db_table):
    data_api = data_api_v2
    ds = create_basic_dataset(
        api_v1=api_v1, connection_id=any_db_saved_connection.uuid,
        data_source_settings=data_source_settings_from_table(table=any_db_table),
        formulas={
            'sum': 'SUM([int_value])',
            'ago': 'AGO([sum], [date_value], "day", 2)',
        }
    )

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='date_value'),
            ds.find_field(title='sum'),
            ds.find_field(title='ago'),
        ],
        order_by=[
            ds.find_field(title='date_value'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)

    _check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=2, day_offset=2)


def test_triple_ago_any_db(any_db_saved_connection, api_v1, data_api_v2, any_db_table):
    data_api = data_api_v2
    ds = create_basic_dataset(
        api_v1=api_v1, connection_id=any_db_saved_connection.uuid,
        data_source_settings=data_source_settings_from_table(table=any_db_table),
        formulas={
            'sum': 'SUM([int_value])',
            'ago_1': 'AGO([sum], [date_value], "day", 1)',
            'ago_2': 'AGO([sum], [date_value], "day", 2)',
            'ago_3': 'AGO([sum], [date_value], "day", 3)',
        }
    )

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='date_value'),
            ds.find_field(title='sum'),
            ds.find_field(title='ago_1'),
            ds.find_field(title='ago_2'),
            ds.find_field(title='ago_3'),
        ],
        order_by=[
            ds.find_field(title='date_value'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)

    _check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=2, day_offset=1)
    _check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=3, day_offset=2)
    _check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=4, day_offset=3)


def test_ago_any_db_multisource(any_db_saved_connection, api_v1, data_api_v2, any_db_two_tables):
    data_api = data_api_v2
    connection_id = any_db_saved_connection.uuid
    table_1, table_2 = any_db_two_tables
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=connection_id,
        **data_source_settings_from_table(table=table_1),
    )
    ds.sources['source_2'] = ds.source(
        connection_id=connection_id,
        **data_source_settings_from_table(table=table_2),
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds.source_avatars['avatar_2'] = ds.sources['source_2'].avatar()
    ds.avatar_relations['rel_1'] = ds.source_avatars['avatar_1'].join(
        ds.source_avatars['avatar_2']
    ).on(
        ds.col('string_value') == ds.col('string_value')
    )

    ds.result_schema['date_1'] = ds.source_avatars['avatar_1'].field(source='date_value')
    ds.result_schema['int_2'] = ds.source_avatars['avatar_2'].field(source='int_value')
    ds.result_schema['sum'] = ds.field(formula='SUM([int_2])')
    ds.result_schema['ago'] = ds.field(formula='AGO([sum], [date_1], "day", 2)')
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    ds = api_v1.save_dataset(ds).dataset

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='date_1'),
            ds.find_field(title='sum'),
            ds.find_field(title='ago'),
        ],
        order_by=[
            ds.find_field(title='date_1'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)

    _check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=2, day_offset=2)


def test_nested_ago(any_db_saved_connection, api_v1, data_api_v2, any_db_table):
    data_api = data_api_v2
    ds = create_basic_dataset(
        api_v1=api_v1, connection_id=any_db_saved_connection.uuid,
        data_source_settings=data_source_settings_from_table(table=any_db_table),
        formulas={
            'sum': 'SUM([int_value])',
            'ago_1': 'AGO([sum], [date_value], "day", 1)',
            'ago_2': 'AGO([ago_1], [date_value], "day", 1)',
            'ago_3': 'AGO([ago_2], [date_value], "day", 1)',
        }
    )

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='date_value'),
            ds.find_field(title='sum'),
            ds.find_field(title='ago_1'),
            ds.find_field(title='ago_2'),
            ds.find_field(title='ago_3'),
        ],
        order_by=[
            ds.find_field(title='date_value'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)

    _check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=2, day_offset=1)
    _check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=3, day_offset=2)
    _check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=4, day_offset=3)


def test_ago_errors(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Ago Sum': 'AGO([Sales Sum], [Order Date], "day", 1)',
    })

    # Dimension in AGO doesn't match dimensions in the query
    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Category'),
            ds.find_field(title='Sales Sum'),
            ds.find_field(title='Ago Sum'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.BAD_REQUEST, result_resp.json
    # FIXME: The more specific error for AGO is temporarily reverted to the generic inconsistent agg error
    assert result_resp.bi_status_code == 'ERR.DS_API.FORMULA.VALIDATION.AGG.INCONSISTENT'
    # assert result_resp.bi_status_code == 'ERR.DS_API.FORMULA.VALIDATION.LOOKUP_FUNC.UNSELECTED_DIMENSION'

    # There are no dimensions in the query
    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Ago Sum'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.BAD_REQUEST, result_resp.json
    # FIXME: Same as above
    assert result_resp.bi_status_code == 'ERR.DS_API.FORMULA.VALIDATION.AGG.INCONSISTENT'
    # assert result_resp.bi_status_code == 'ERR.DS_API.FORMULA.VALIDATION.LOOKUP_FUNC.UNSELECTED_DIMENSION'


def test_ago_in_compeng(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales RSum': 'RSUM(SUM([Sales]))',
        'Ago RSum': 'AGO([Sales RSum], [Order Date], "day", 1)',
    })

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Order Date'),
            ds.find_field(title='Sales RSum'),
            ds.find_field(title='Ago RSum'),
        ],
        order_by=[
            ds.find_field(title='Order Date'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)
    _check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=2, day_offset=1)


def test_dimensions_in_ago_identical_to_dims_in_query(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Order Date Clone': '[Order Date]',
        'Group Sales': 'SUM([Sales])',
        'Ago Along Clone': 'AGO([Group Sales], [Order Date Clone])',
    })

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Order Date'),
            ds.find_field(title='Group Sales'),
            ds.find_field(title='Ago Along Clone'),
        ],
        order_by=[
            ds.find_field(title='Order Date'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)
    _check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=2, day_offset=1)


def test_ago_with_non_ago_aggregation(api_v1, data_api_v2, dataset_id):
    """
    Check that an expression containing ago and a simple aggregation
    at the same level is sliced correctly.
    """
    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Group Sales': 'SUM([Sales])',
        'Ago': 'AGO([Group Sales], [Order Date])',
        'Ago And not Ago Agg': '[Group Sales] - [Ago]',
        # In this formula the left part ([Group Sales]) has no AGO (QueryFork) nodes in it,
        # while the right part ([Ago]) does.
        # This means that fork slicing will be used, but it needs to slice above aggregations (not below)
        # in the parts of the expression where there are no QueryFork nodes
    })

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Order Date'),
            ds.find_field(title='Group Sales'),
            ds.find_field(title='Ago And not Ago Agg'),
        ],
        order_by=[
            ds.find_field(title='Order Date'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json


def test_ago_with_avatarless_measure(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Avatarless Measure': 'COUNT()',
        'Ago': 'AGO([Avatarless Measure], [Order Date])',
    })

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Order Date'),
            ds.find_field(title='Avatarless Measure'),
            ds.find_field(title='Ago'),
        ],
        order_by=[
            ds.find_field(title='Order Date'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)
    _check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=2, day_offset=1)


def test_ago_with_ignore_dimensions(api_v1, data_api_v2, connection_id, clickhouse_table):
    data_api = data_api_v2
    ds = create_basic_dataset(
        api_v1=api_v1, connection_id=connection_id,
        data_source_settings=data_source_settings_from_table(table=clickhouse_table),
        formulas={
            'sum': 'SUM([int_value])',
            # Create a dimension that would cause regular AGO to always return NULL:
            'day': 'DAY([date_value])',
            # Regular AGO:
            'ago': 'AGO([sum], [date_value])',
            # AGO that ignores [day] in JOIN
            'ago_igdim': 'AGO([sum], [date_value] IGNORE DIMENSIONS [day])',
        },
    )

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='date_value'),
            ds.find_field(title='sum'),
            ds.find_field(title='ago'),
            ds.find_field(title='ago_igdim'),
            # "Bad" dimension:
            ds.find_field(title='day'),
        ],
        order_by=[
            ds.find_field(title='date_value'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)

    # [ago] should have all NULL values because of the "bad" dimension [day]
    for row_idx, row in enumerate(data_rows):
        assert row[2] is None, f'Expected a None value in row {row_idx}, but got {row[2]}'

    # [ago_igdim] should act the same way as a regular AGO under regular circumstances
    _check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=3, day_offset=1)


def test_at_date(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Sales Sum Fixed': 'AT_DATE([Sales Sum], [Order Date], #2014-02-02#)',
        'Sales Sum Nullable': 'AT_DATE([Sales Sum], [Order Date], DATE_PARSE("2014-02-02"))',
        'Sales Sum Trunc': 'AT_DATE([Sales Sum], [Order Date], DATETRUNC([Order Date], "month"))',
    })

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Category'),
            ds.find_field(title='Order Date'),
            ds.find_field(title='Sales Sum'),
            ds.find_field(title='Sales Sum Fixed'),
            ds.find_field(title='Sales Sum Nullable'),
            ds.find_field(title='Sales Sum Trunc'),
        ],
        order_by=[
            ds.find_field(title='Category'),
            ds.find_field(title='Order Date'),
        ],
        filters=[
            ds.find_field(title='Category').filter(op=WhereClauseOperation.EQ, values=['Office Supplies']),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)
    _check_at_date_data(data_rows=data_rows, date_idx=1, value_idx=2, ago_idx=3,
                        ago_date_callable=lambda d: datetime.date(2014, 2, 2))
    _check_at_date_data(data_rows=data_rows, date_idx=1, value_idx=2, ago_idx=4,
                        ago_date_callable=lambda d: datetime.date(2014, 2, 2))
    _check_at_date_data(data_rows=data_rows, date_idx=1, value_idx=2, ago_idx=5,
                        ago_date_callable=lambda d: d.replace(day=1))


def test_at_date_with_measure_as_third_arg(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2

    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Sales Sum': 'SUM([Sales])',
        'Profit Sum': 'SUM([Profit])',
        'Sum At Date': 'AT_DATE([Sales Sum], [Order Date], #2014-02-02# + [Profit Sum]*0)',
    })

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Category'),
            ds.find_field(title='Order Date'),
            ds.find_field(title='Sales Sum'),
            ds.find_field(title='Sum At Date'),
        ],
        order_by=[
            ds.find_field(title='Category'),
            ds.find_field(title='Order Date'),
        ],
        filters=[
            ds.find_field(title='Category').filter(op=WhereClauseOperation.EQ, values=['Office Supplies']),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)
    _check_at_date_data(data_rows=data_rows, date_idx=1, value_idx=2, ago_idx=3,
                        ago_date_callable=lambda d: datetime.date(2014, 2, 2))


def test_ago_with_bfb(api_v1, data_api_v2, connection_id, clickhouse_table):
    data_api = data_api_v2
    day_offset = 3

    ds = create_basic_dataset(
        api_v1=api_v1, connection_id=connection_id,
        data_source_settings=data_source_settings_from_table(table=clickhouse_table),
        formulas={
            'sum': 'SUM([int_value])',
            'date_duplicate': '[date_value]',
            'ago': f'AGO([sum], [date_value], "day", {day_offset})',
            'ago_bfb': (
                f'AGO([sum], [date_value], "day", {day_offset} '
                f'BEFORE FILTER BY [date_duplicate])'
            ),
            'ago_bfb_nested': (
                f'AGO(AGO([sum], [date_value], "day", 1), [date_value], "day", {day_offset - 1} '
                'BEFORE FILTER BY [date_duplicate])'
            ),
        }
    )

    min_date_s, _ = get_range_values(data_api.get_value_range(dataset=ds, field=ds.find_field(title='date_value')))
    min_date = _read_date(min_date_s)
    gte_date_s = (min_date + datetime.timedelta(days=day_offset)).isoformat()

    def get_data_rows_with_filter(
            filters: Optional[list[WhereClause]] = None,
    ) -> list[list]:
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title='date_value'),
                ds.find_field(title='sum'),
                ds.find_field(title='ago'),
                ds.find_field(title='ago_bfb'),
                ds.find_field(title='ago_bfb_nested'),
            ],
            order_by=[
                ds.find_field(title='date_value'),
            ],
            filters=filters,
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        return get_data_rows(result_resp)

    # Check default BFB (main dimension) in AGO without explicit BFB
    data_rows = get_data_rows_with_filter(
        filters=[
            # Apply filter to main dimension - it should be BFBed by default
            ds.find_field(title='date_value').filter(
                # > min_date + {day_offset} days
                op=WhereClauseOperation.GTE, values=[gte_date_s]
            ),
        ]
    )
    _check_ago_data(
        data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=2, day_offset=day_offset,
        allow_missing_date_values=True,
    )
    # Now make sure there really are non-NULL values
    for row_idx in range(1, day_offset):  # Skip 0th row because because it has no AGO value
        assert data_rows[row_idx][2] is not None, f'Expected a non-None value in row {row_idx}'

    # Now check the explicit BFB (with filter for non-main dimension)
    data_rows = get_data_rows_with_filter(
        filters=[
            # Filter has to be applied to a dimension other than the one in AGO
            ds.find_field(title='date_duplicate').filter(
                # > min_date + {day_offset} days
                op=WhereClauseOperation.GTE, values=[gte_date_s]
            ),
        ]
    )
    _check_ago_data(
        data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=2, day_offset=day_offset,
    )
    # Omit the first 2 rows because their values are not None
    _check_ago_data(
        data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=3, day_offset=day_offset,
        allow_missing_date_values=True,
    )
    _check_ago_data(
        data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=4, day_offset=day_offset,
        allow_missing_date_values=True,
    )
    # Now make sure there really are non-NULL values
    for row_idx in range(1, day_offset):  # Skip 0th row because because it has no AGO value
        assert data_rows[row_idx][3] is not None, f'Expected a non-None value in row {row_idx}'
        assert data_rows[row_idx][4] is not None, f'Expected a non-None value in row {row_idx}'


def test_ago_with_corner_case_dimensions(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Measure': 'SUM([Sales])',
        'Invalid Field': '[Whaaa?...]',
        'Invalid AGO': 'AGO([Measure], [Invalid Field])',
    }, exp_status=HTTPStatus.BAD_REQUEST)

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Order Date'),
            ds.find_field(title='Invalid Field'),
            ds.find_field(title='Invalid AGO'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.BAD_REQUEST


def test_ago_with_different_measures(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(api_v1=api_v1, dataset_id=dataset_id, formulas={
        'Measure 1': 'SUM([Sales])',
        'Measure 2': 'COUNTD([Sales])',
        'Ago Measure 1': 'AGO([Measure 1], [Order Date])',
        'Ago Measure 2': 'AGO([Measure 2], [Order Date])',
    })

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Order Date'),
            ds.find_field(title='Measure 1'),
            ds.find_field(title='Measure 2'),
            ds.find_field(title='Ago Measure 1'),
            ds.find_field(title='Ago Measure 2'),
        ],
        order_by=[
            ds.find_field(title='Order Date'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)
    _check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=3, day_offset=1)
    _check_ago_data(data_rows=data_rows, date_idx=0, value_idx=2, ago_idx=4, day_offset=1)


def test_month_ago_for_shorter_month(any_db, any_db_saved_connection, api_v1, data_api_v2, any_db_table_100):
    data_api = data_api_v2

    if any_db.conn_type == CONNECTION_TYPE_ORACLE:
        # Oracle cannot add a month to 2021-01-31 (2021-02-31 doesn't exist)
        pytest.skip()

    ds = create_basic_dataset(
        api_v1=api_v1, connection_id=any_db_saved_connection.uuid,
        data_source_settings=data_source_settings_from_table(table=any_db_table_100),
        formulas={
            'new_date_value': '#2021-01-01# + [int_value]',
            'sum': 'SUM([int_value])',
            'ago': 'AGO([sum], [new_date_value], "month", 1)',
        }
    )

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='new_date_value'),
            ds.find_field(title='sum'),
            ds.find_field(title='ago'),
        ],
        filters=[
            ds.find_field(title='new_date_value').filter(op=WhereClauseOperation.EQ, values=['2021-02-28']),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)

    # Check whether rows are duplicated
    assert len(data_rows) == 1
