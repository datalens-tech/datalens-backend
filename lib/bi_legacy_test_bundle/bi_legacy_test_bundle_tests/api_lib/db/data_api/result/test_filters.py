from __future__ import annotations

import json
from http import HTTPStatus
from typing import Any, Callable, Set

from bi_constants.enums import WhereClauseOperation, BIType

from bi_api_client.dsmaker.primitives import Dataset
from bi_api_client.dsmaker.shortcuts.result_data import get_data_rows

from bi_core_testing.database import make_table_with_arrays, C

from bi_legacy_test_bundle_tests.api_lib.utils import data_source_settings_from_table


def test_get_result_with_formula_in_where(api_v1, data_api_v1, dataset_id):
    data_api = data_api_v1
    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset

    ds.result_schema['Discount Percentage'] = ds.field(formula='[Discount] * 100')
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(ds).dataset

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Discount Percentage'),
        ],
        where=[
            ds.find_field(title='Discount Percentage').filter(WhereClauseOperation.GT, [30]),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)
    assert data_rows
    assert all(float(row[0]) > 30 for row in data_rows)


def test_get_result_with_string_filter_operations_for_integer(api_v1, data_api_v1, dataset_id):
    data_api = data_api_v1
    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset

    ds.result_schema['Int Measure'] = ds.field(formula='COUNT()')
    ds_resp = api_v1.apply_updates(dataset=ds)
    ds = ds_resp.dataset
    options = ds_resp.json['options']
    int_options = next(dt for dt in options['data_types']['items'] if dt['type'] == 'integer')
    assert 'ICONTAINS' in int_options['filter_operations']

    ds = api_v1.save_dataset(ds).dataset

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Int Measure'),
            ds.find_field(title='City'),
        ],
        where=[
            ds.find_field(title='Int Measure').filter(WhereClauseOperation.ICONTAINS, ['2']),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)
    values: Set[str] = {row[0] for row in data_rows}
    assert len(values) > 1  # we just need to make sure there are several different values
    assert all('2' in value for value in values)


def test_filter_from_different_avatar(connection_id, api_v1, data_api_v1, two_clickhouse_tables):
    """
    Test usage of filter that references an avatar
    that is not used by any other expressions.
    """

    data_api = data_api_v1
    table_1, table_2 = two_clickhouse_tables
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

    ds.result_schema['int_1'] = ds.source_avatars['avatar_1'].field(source='int_value')
    ds.result_schema['int_2'] = ds.source_avatars['avatar_2'].field(source='int_value')
    ds.result_schema['sum'] = ds.field(formula='COUNT()')
    ds = api_v1.apply_updates(dataset=ds, fail_ok=True).dataset
    ds = api_v1.save_dataset(ds).dataset

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='int_1'),
            ds.find_field(title='sum'),
        ],
        group_by=[
            ds.find_field(title='int_1'),
        ],
        where=[
            ds.find_field(title='int_2').filter(WhereClauseOperation.EQ, [5]),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)

    assert len(data_rows) == 1


def test_array_len_filters(api_v1, data_api_v1, clickhouse_db, connection_id):
    data_api = data_api_v1
    db = clickhouse_db
    columns = [
        C('int_value', BIType.integer, vg=lambda rn, **kwargs: rn),
        C('array_int_value', BIType.array_int, vg=lambda rn, **kwargs: [i for i in range(rn)]),
        C('array_str_value', BIType.array_str, vg=lambda rn, **kwargs: [str(i) for i in range(rn)]),
        C('array_float_value', BIType.array_float, vg=lambda rn, **kwargs: [i/100.0 for i in range(rn)]),
    ]
    db_table = make_table_with_arrays(db, columns=columns)
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=connection_id, **data_source_settings_from_table(db_table))
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset

    ds.result_schema['tree_str_value'] = ds.field(formula='TREE([array_str_value])')
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(ds).dataset

    num = 3

    def check_filter(
            field_title: str, filter_op: WhereClauseOperation, comp_func: Callable[[int], bool],
    ) -> None:
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title='int_value'),
                ds.find_field(title=field_title),
            ],
            where=[
                ds.find_field(title=field_title).filter(filter_op, [num]),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert data_rows
        assert all(comp_func(len(json.loads(row[1]))) for row in data_rows)

    check_filter('array_int_value', filter_op=WhereClauseOperation.LENEQ, comp_func=(lambda x: x == num))
    check_filter('array_str_value', filter_op=WhereClauseOperation.LENEQ, comp_func=(lambda x: x == num))
    check_filter('array_float_value', filter_op=WhereClauseOperation.LENEQ, comp_func=(lambda x: x == num))
    check_filter('tree_str_value', filter_op=WhereClauseOperation.LENEQ, comp_func=(lambda x: x == num))

    check_filter('array_int_value', filter_op=WhereClauseOperation.LENNE, comp_func=(lambda x: x != num))
    check_filter('array_int_value', filter_op=WhereClauseOperation.LENGT, comp_func=(lambda x: x > num))
    check_filter('array_int_value', filter_op=WhereClauseOperation.LENGTE, comp_func=(lambda x: x >= num))
    check_filter('array_str_value', filter_op=WhereClauseOperation.LENLT, comp_func=(lambda x: x < num))
    check_filter('array_str_value', filter_op=WhereClauseOperation.LENLTE, comp_func=(lambda x: x <= num))


def test_array_startswith_filter(api_v1, data_api_v1, clickhouse_db, connection_id):
    data_api = data_api_v1
    db = clickhouse_db
    columns = [
        C('int_value', BIType.integer, vg=lambda rn, **kwargs: rn),
        C('array_int_value', BIType.array_int, vg=lambda rn, **kwargs: [i for i in reversed(range(rn))]),
        C('array_str_value', BIType.array_str, vg=lambda rn, **kwargs: [str(i) for i in reversed(range(rn))]),
        C('array_float_value', BIType.array_float, vg=lambda rn, **kwargs: [i/100.0 for i in reversed(range(rn))]),
    ]
    db_table = make_table_with_arrays(db, columns=columns)
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=connection_id, **data_source_settings_from_table(db_table))
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(ds).dataset

    def check_filter(field_title: str, filter_value: list) -> None:
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title='int_value'),
                ds.find_field(title=field_title),
            ],
            where=[
                ds.find_field(title=field_title).filter(
                    op=WhereClauseOperation.STARTSWITH,
                    values=[json.dumps(filter_value)],
                ),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) == 1
        result_array = json.loads(data_rows[0][1])
        assert result_array[:len(filter_value)] == filter_value

    check_filter('array_int_value', filter_value=[3, 2, 1, 0])
    check_filter('array_float_value', filter_value=[0.03, 0.02, 0.01, 0.0])
    check_filter('array_str_value', filter_value=['3', '2', '1', '0'])


def test_array_contains_filter(api_v1, data_api_v1, clickhouse_db, connection_id):
    data_api = data_api_v1
    db = clickhouse_db
    columns = [
        C('int_value', BIType.integer, vg=lambda rn, **kwargs: rn),
        C('array_int_value', BIType.array_int, vg=lambda rn, **kwargs: [i for i in reversed(range(rn))]),
        C('array_str_value', BIType.array_str, vg=lambda rn, **kwargs: [str(i) for i in reversed(range(rn))]),
        C('array_float_value', BIType.array_float, vg=lambda rn, **kwargs: [i/100.0 for i in reversed(range(rn))]),
    ]
    db_table = make_table_with_arrays(db, columns=columns)
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=connection_id, **data_source_settings_from_table(db_table))
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(ds).dataset

    def check_filter(field_title: str, filter_value: Any, not_: bool = False) -> None:
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title='int_value'),
                ds.find_field(title=field_title),
            ],
            where=[
                ds.find_field(title=field_title).filter(
                    op=WhereClauseOperation.CONTAINS if not not_ else WhereClauseOperation.NOTCONTAINS,
                    values=[str(filter_value)],
                ),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert data_rows
        for row in data_rows:
            result_array = json.loads(row[1])
            if not not_:
                assert filter_value in result_array
            else:
                assert filter_value not in result_array

    check_filter('array_int_value', filter_value=3)
    check_filter('array_int_value', filter_value=3, not_=True)
    check_filter('array_float_value', filter_value=0.03)
    check_filter('array_float_value', filter_value=0.03, not_=True)
    check_filter('array_str_value', filter_value='3')
    check_filter('array_str_value', filter_value='3', not_=True)
