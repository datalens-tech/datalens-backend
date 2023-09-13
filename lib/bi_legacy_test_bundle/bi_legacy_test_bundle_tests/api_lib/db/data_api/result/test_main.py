from __future__ import annotations

import uuid
from http import HTTPStatus
from typing import List, Optional

import pytest

from bi_constants.enums import WhereClauseOperation

from bi_api_client.dsmaker.primitives import Dataset, ObligatoryFilter, WhereClause
from bi_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from bi_api_client.dsmaker.api.data_api import SyncHttpDataApiV1
from bi_api_client.dsmaker.shortcuts.result_data import get_data_rows
from bi_testing.utils import guids_from_titles

from bi_connector_greenplum.core.constants import SOURCE_TYPE_GP_TABLE
from bi_connector_clickhouse.core.clickhouse.constants import SOURCE_TYPE_CH_TABLE

from bi_legacy_test_bundle_tests.api_lib.utils import (
    get_result_schema, get_field_by_title, data_source_settings_from_table,
)


def test_get_dataset_version_result(client, data_api_v1_test_mutation_cache, static_dataset_id):
    data_api_v1 = data_api_v1_test_mutation_cache

    def title_to_id(title: str) -> str:
        nonlocal result_schema
        return guids_from_titles(result_schema, [title])[0]

    dataset_id = static_dataset_id
    result_schema = get_result_schema(client, dataset_id)
    req_data = {
        'columns': guids_from_titles(result_schema, ['Discount', 'City']),
        'where': [{'column': title_to_id('Discount'), 'operation': 'EQ', 'values': [0]}]
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)
    assert all(float(item[0]) == 0.0 for item in r.json['result']['data']['Data'])
    assert not r.json['result']['data_export_forbidden']
    assert r.json['result']['fields']

    req_data = {
        'columns': guids_from_titles(result_schema, ['Discount', 'City']),
        'where': [{'column': title_to_id('City'), 'operation': 'IN', 'values': ['Concord', 'Seattle']}]
    }
    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)
    assert len(r.json['result']['data']['Data']) != 0
    assert all(item[1] in ['Concord', 'Seattle'] for item in r.json['result']['data']['Data'])

    req_data = {
        'columns': guids_from_titles(result_schema, ['Discount', 'City']),
        'where': [{'column': title_to_id('Discount'), 'operation': 'GT', 'values': [0]}]
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)
    assert len(r.json['result']['data']['Data']) != 0
    assert all(float(item[0]) > 0.0 for item in r.json['result']['data']['Data'])

    req_data = {
        'columns': guids_from_titles(result_schema, ['Discount', 'City']),
        'where': [{'column': title_to_id('Discount'), 'operation': 'BETWEEN', 'values': [0, 0.5]}]
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)
    assert r.status_code == 200
    assert all(0.0 <= float(item[0]) <= 0.5 for item in r.json['result']['data']['Data'])

    req_data = {
        'columns': guids_from_titles(result_schema, ['Discount', 'City']),
        'where': [{'column': title_to_id('Discount'), 'operation': 'NE', 'values': [0]}]
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)
    assert r.status_code == 200
    assert all(float(item[0]) != 0.0 for item in r.json['result']['data']['Data'])

    req_data = {
        'columns': guids_from_titles(result_schema, ['Discount', 'City']),
        'where': [{'column': title_to_id('Discount'), 'operation': 'NIN', 'values': [0, 0.5]}]
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)
    assert r.status_code == 200
    assert all(float(item[0]) not in [0.0, 0.5] for item in r.json['result']['data']['Data'])

    req_data = {
        'columns': guids_from_titles(result_schema, ['Order Date']),
        'where': [{'column': title_to_id('Order Date'),
                   'operation': 'BETWEEN', 'values': ['2014-01-05', '2014-01-07']}]
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)
    assert r.status_code == 200
    assert all(item[0] in ['2014-01-05', '2014-01-06', '2014-01-07'] for item in r.json['result']['data']['Data'])


def test_get_dataset_version_result_distinct(client, data_api_v1_test_mutation_cache, static_dataset_id):
    data_api_v1 = data_api_v1_test_mutation_cache
    # FIXME: What does this test test????
    dataset_id = static_dataset_id
    result_schema = get_result_schema(client, dataset_id)
    req_data = {
        'columns': guids_from_titles(result_schema, ['City']),
        'group_by': guids_from_titles(result_schema, ['City']),
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)

    assert r.status_code == 200

    cities = [x[0] for x in r.json['result']['data']['Data']]
    assert len(cities) == len(set(cities))


@pytest.mark.parametrize('ref_type', ('id', 'title'))
def test_incomplete_field_ref(client, data_api_v2, static_dataset_id, ref_type):
    dataset_id = static_dataset_id

    req_data = {
        'fields': [{
            'ref': {
                'type': ref_type,
            },
        }],
        'updates': [],
    }

    r = data_api_v2.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)

    assert r.status_code == 400
    assert r.json == {
        'code': 'ERR.DS_API',
        'debug': {},
        'details': {},
        'message': f"{{'fields': {{0: {{'ref': {{'{ref_type}': ['Missing data for required field.']}}}}}}}}",
    }


def test_calcmode_formula_without_formula_field(client, data_api_v1_5, static_dataset_id):
    dataset_id = static_dataset_id

    guid = str(uuid.uuid4())
    batch = [
        {
            'action': 'add_field',
            'field': {
                'guid': guid,
                'title': 'abc',
                'calc_mode': 'formula',
                'description': '',
                'strict': False,
                'hidden': False,
                'cast': 'float',
            },
        }
    ]
    req_data = {
        'fields': [{
            'ref': {
                'type': 'id',
                'id': guid,
            },
        }],
        'updates': batch,
    }

    r = data_api_v1_5.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)

    assert r.status_code == 400
    assert r.json == {
        'code': 'ERR.DS_API.FORMULA.PARSE.UNEXPECTED_EOF.EMPTY_FORMULA',
        'debug': {},
        'details': {},
        'message': "Empty formula",
    }


def test_get_dataset_version_result_field_with_dependencies(api_v1, data_api_all_v, static_dataset_id):
    data_api = data_api_all_v
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    ds.result_schema['New shiny field'] = ds.field(formula='[Quantity] + 10')

    result_resp = data_api.get_result(dataset=ds, fields=[ds.result_schema['New shiny field']])
    assert result_resp.status_code == HTTPStatus.OK


def test_get_dataset_version_result_with_group_by(client, data_api_v1, static_dataset_id):
    # FIXME: Test is a bit pointless now, probably can be removed
    dataset_id = static_dataset_id
    result_schema = get_result_schema(client, dataset_id)
    batch = [{'action': 'update', 'field': {
        'guid': result_schema[0]['guid'],
        'aggregation': 'count',
    }}]

    req_data = {
        'updates': batch,
        'columns': [x['guid'] for x in result_schema],
        'group_by': [result_schema[i]['guid'] for i in range(1, len(result_schema))],
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)

    assert r.status_code == 200


def test_get_dataset_version_result_with_duplicate_columns(api_v1, data_api_all_v, static_dataset_id):
    data_api = data_api_all_v
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    result_resp = data_api.get_result(
        dataset=ds, fields=[field for field in ds.result_schema] * 2
    )
    assert result_resp.status_code == HTTPStatus.OK


def test_get_sorted_dsv_result(api_v1, data_api_all_v, static_dataset_id):
    data_api = data_api_all_v
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Discount'),
            ds.find_field(title='City'),
        ],
        order_by=[
            ds.find_field(title='Discount').desc,
        ]
    )
    assert result_resp.status_code == HTTPStatus.OK
    data = get_data_rows(result_resp)
    assert all(data[i][0] <= data[i - 1][0] for i in range(1, len(data)))


def test_get_dsv_result_with_limit(api_v1, data_api_all_v, static_dataset_id):
    data_api = data_api_all_v
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    LIMIT = 3
    result_resp = data_api.get_result(
        dataset=ds, fields=[
            ds.find_field(title='Discount'),
            ds.find_field(title='City'),
        ],
        limit=LIMIT,
    )
    assert result_resp.status_code == HTTPStatus.OK
    data = get_data_rows(result_resp)
    assert len(data) == LIMIT


def test_get_dsv_result_with_offset(api_v1, data_api_all_v, static_dataset_id):
    data_api = data_api_all_v
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    OFFSET = 5
    LIMIT_0 = 15
    LIMIT = 10

    def get_data(limit: Optional[int] = None, offset: Optional[int] = None) -> List[list]:
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title='Order ID'),
                ds.find_field(title='Discount'),
                ds.find_field(title='City'),
            ],
            order_by=[
                ds.find_field(title='Order ID').desc,
            ],
            limit=limit,
            offset=offset,
        )
        assert result_resp.status_code == HTTPStatus.OK
        data = get_data_rows(result_resp)
        return data

    data1 = get_data(limit=LIMIT_0)
    data2 = get_data(limit=LIMIT, offset=OFFSET)

    assert data1[OFFSET:OFFSET+LIMIT] == data2


def test_get_result_profit_per_order(client, data_api_v1, static_dataset_id):
    dataset_id = static_dataset_id
    result_schema = get_result_schema(client, dataset_id)

    profit_field = get_field_by_title(result_schema, title='Profit')
    batch = [
        {'action': 'update', 'field': {'guid': profit_field['guid'], 'aggregation': 'sum'}},
        {'action': 'add', 'field': {
            'guid': str(uuid.uuid4()),
            'title': 'Order Count',
            'avatar_id': profit_field['avatar_id'],
            'source': 'Order ID',
            'calc_mode': 'direct',
            'aggregation': 'countunique',
        }},
        {'action': 'add', 'field': {
            'guid': str(uuid.uuid4()),
            'title': 'Profit per order',
            'calc_mode': 'formula',
            'formula': '[Profit]/[Order Count]',
        }},
    ]

    req_data = {
        'updates': batch,
        'columns': guids_from_titles(result_schema + batch, ['Order Date', 'Profit per order']),
        'where': [{
            'column': guids_from_titles(batch, ['Profit per order'])[0],
            'operation': 'GT', 'values': [0]
        }]
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)

    assert r.status_code == 200


def test_get_result_select_date_formula(data_api_v1, static_dataset_id):
    dataset_id = static_dataset_id

    batch = [
        {
            'action': 'add',
            'field': {
                'guid': str(uuid.uuid4()),
                'title': 'abc',
                'calc_mode': 'formula',
                'formula': 'YEAR(#2016-01-01#)'
            }
        }
    ]
    req_data = {
        'updates': batch,
        'columns': guids_from_titles(batch, ['abc']),
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)

    assert r.status_code == 200
    data = r.json['result']['data']['Data']
    assert data == [['2016']]


def test_get_result_filtered_by_measure(client, data_api_v1, static_dataset_id):
    dataset_id = static_dataset_id
    result_schema = get_result_schema(client, dataset_id)

    profit_field = get_field_by_title(result_schema, title='Profit')
    batch = [
        {'action': 'update', 'field': {'guid': profit_field['guid'], 'aggregation': 'sum'}},
    ]

    req_data = {
        'updates': batch,
        'columns': guids_from_titles(result_schema, ["Sub-Category"]),
        'where': [{
            "column": guids_from_titles(result_schema, ["Profit"])[0],
            "operation": "GT",
            "values": [0]
        }]
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)

    assert r.status_code == 200


def test_get_result_filtered_by_measure_of_formula(client, data_api_v1, static_dataset_id):
    dataset_id = static_dataset_id
    result_schema = get_result_schema(client, dataset_id)

    profit_field = get_field_by_title(result_schema, title='Profit')
    batch = [
        {'action': 'update', 'field': {
            'guid': profit_field['guid'],
            'aggregation': 'sum',
            'formula': '[Quantity] + 1',
            'calc_mode': 'formula',
        }},
    ]

    req_data = {
        'updates': batch,
        'columns': guids_from_titles(result_schema, ["Sub-Category"]),
        'where': [{
            "column": guids_from_titles(result_schema, ["Profit"])[0],
            "operation": "GT",
            "values": [0]
        }]
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)

    assert r.status_code == 200


def test_get_nonexisting_field(data_api_v1, static_dataset_id):
    dataset_id = static_dataset_id
    random_uuid = str(uuid.uuid4())
    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body={'columns': [random_uuid]})
    assert r.status_code == HTTPStatus.BAD_REQUEST
    assert r.json['code'] == 'ERR.DS_API.FIELD.NOT_FOUND'


def test_filter_by_nonexistent_field(client, data_api_v1, static_dataset_id):
    dataset_id = static_dataset_id
    result_schema = get_result_schema(client, dataset_id)
    req_data = {
        'columns': guids_from_titles(result_schema, ['Discount', 'City']),
        'where': [{'column': '0000000000000000', 'operation': 'EQ', 'values': [0]}]
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)

    assert r.status_code == HTTPStatus.BAD_REQUEST

    req_data = {
        'columns': guids_from_titles(result_schema, ['Discount', 'City']),
        'where': [{'column': '0000000000000000', 'operation': 'EQ', 'values': [0]}],
        'ignore_nonexistent_filters': True,
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)

    assert r.status_code == HTTPStatus.OK


def test_multisource_dataset(api_v1, data_api_v1, static_connection_id, three_clickhouse_tables):
    table_1, table_2, table_3 = three_clickhouse_tables
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=static_connection_id, **data_source_settings_from_table(table=table_1))
    ds.sources['source_2'] = ds.source(
        connection_id=static_connection_id, **data_source_settings_from_table(table=table_2))
    ds.sources['source_3'] = ds.source(
        connection_id=static_connection_id, **data_source_settings_from_table(table=table_3))
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds.source_avatars['avatar_2'] = ds.sources['source_2'].avatar()
    ds.source_avatars['avatar_3'] = ds.sources['source_3'].avatar()
    ds.avatar_relations['relation_1'] = ds.source_avatars['avatar_1'].join(
        ds.source_avatars['avatar_2']
    ).on(
        ds.col('int_value') == ds.col('int_value'),
    )
    ds.avatar_relations['relation_2'] = ds.source_avatars['avatar_1'].join(
        ds.source_avatars['avatar_3']
    ).on(
        ds.col('int_value') == ds.col('int_value'),
    )
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset

    result_data = data_api_v1.get_result(dataset=ds, limit=7, fields=[
        ds.find_field(title='int_value'),
        ds.find_field(title='int_value (1)'),
        ds.find_field(title='datetime_value (1)'),
        ds.find_field(title='int_value (2)'),
    ]).data
    assert len(result_data['Data']) == 7
    titles = set(col_schema[0] for col_schema in result_data['Type'][1][1])
    assert titles == {'int_value', 'int_value (1)', 'datetime_value (1)', 'int_value (2)'}


def test_obligatory_filter_check(api_v1: SyncHttpDatasetApiV1, data_api_v1, ch_data_source_settings):
    ds = Dataset()
    ds.sources['source_1'] = ds.source(**ch_data_source_settings)
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset
    category_field = ds.result_schema[0]

    req_data = {
        'columns': [ds.result_schema[0].id, ds.result_schema[1].id],
        'where': [dict(column=category_field.id, operation='EQ', values=['Office Supplies'])],
        'ignore_nonexistent_filters': True,
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=ds.id, raw_body=req_data)
    assert r.status_code == 200

    filter_id = str(uuid.uuid4())
    ds = api_v1.apply_updates(dataset=ds, updates=[
        ObligatoryFilter(
            id=filter_id,
            field_guid=category_field.id,
            default_filters=[
                WhereClause(column=category_field.id, operation=WhereClauseOperation.IN, values=['Furniture']),
            ],
        ).add()
    ]).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset

    r = data_api_v1.get_response_for_dataset_result(dataset_id=ds.id, raw_body=req_data)
    assert r.status_code == HTTPStatus.OK

    r = data_api_v1.get_response_for_dataset_result(
        dataset_id=ds.id,
        raw_body={'columns': [ds.result_schema[0].id, ds.result_schema[1].id]},
    )
    # # Obligatory filters check was disabled (https://st.yandex-team.ru/BI-2483)
    # assert r.status_code == HTTPStatus.BAD_REQUEST
    # assert r.json['code'] == 'ERR.DS_API.OBLIG_FILTER_MISSING'
    assert r.status_code == HTTPStatus.OK


def test_filter_error(api_v1, data_api_v1, dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset
    ds.result_schema['New bool'] = ds.field(formula='"P" = "NP"')  # just a bool field
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(ds).dataset

    # invalid value for date
    result_resp = data_api_v1.get_result(
        dataset=ds, fields=[ds.find_field(title='Order Date')], fail_ok=True,
        where=[ds.find_field(title='Order Date').filter('EQ', values=['Office Supplies'])],
    )
    assert result_resp.status_code == HTTPStatus.BAD_REQUEST
    assert result_resp.bi_status_code == 'ERR.DS_API.FILTER.INVALID_VALUE'

    # a avery special check for invalid bool values
    result_resp = data_api_v1.get_result(
        dataset=ds, fields=[ds.find_field(title='Order Date')], fail_ok=True,
        where=[ds.result_schema['New bool'].filter('EQ', values=[''])],
    )
    assert result_resp.status_code == HTTPStatus.BAD_REQUEST
    assert result_resp.bi_status_code == 'ERR.DS_API.FILTER.INVALID_VALUE'

    # unknown field
    result_resp = data_api_v1.get_result(
        dataset=ds, fields=[ds.find_field(title='Order Date')], fail_ok=True,
        where=[ds.field(title='Made-up field').filter('EQ', values=['whatever'])],
    )
    assert result_resp.status_code == HTTPStatus.BAD_REQUEST
    assert result_resp.bi_status_code == 'ERR.DS_API.FIELD.NOT_FOUND'


def test_get_result_with_disable_group_by(client, data_api_v1, static_dataset_id):
    dataset_id = static_dataset_id
    result_schema = get_result_schema(client, dataset_id)

    req_data = {
        'columns': guids_from_titles(result_schema, ['Discount', 'City']),
        'limit': 10,
        'disable_group_by': True,
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)
    query = r.json['result']['query']
    assert 'GROUP BY' not in query

    profit_field = get_field_by_title(result_schema, title='Profit')
    batch = [
        {'action': 'update', 'field': {'guid': profit_field['guid'], 'aggregation': 'sum'}},
    ]

    req_data = {
        'updates': batch,
        'columns': guids_from_titles(result_schema, ['Sub-Category', 'Profit']),
        'disable_group_by': True,
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)

    assert r.status_code == HTTPStatus.BAD_REQUEST
    assert r.json['code'] == 'ERR.DS_API.INVALID_GROUP_BY_CONFIGURATION'
    assert r.json['message'] == 'Invalid parameter disable_group_by for dataset with measure fields'


def test_get_result_with_order_by_column_not_in_select(client, data_api_v1, static_dataset_id):
    dataset_id = static_dataset_id
    result_schema = get_result_schema(client, dataset_id)

    req_data = {
        'columns': guids_from_titles(result_schema, ['Discount', 'City']),
        'order_by': [
            {
                'column': guids_from_titles(result_schema, ['Profit'])[0],
                'direction': 'DESC',
            }
        ],
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)

    assert r.status_code == HTTPStatus.BAD_REQUEST

    discount_field = get_field_by_title(result_schema, title='Discount')
    profit_field = get_field_by_title(result_schema, title='Profit')
    batch = [
        {
            'action': 'update',
            'field': {
                'guid': discount_field['guid'],
                'aggregation': 'avg',
            }
        },
        {
            'action': 'update',
            'field': {
                'guid': profit_field['guid'],
                'aggregation': 'sum',
            }
        },
    ]

    req_data = {
        'updates': batch,
        'columns': guids_from_titles(result_schema, ['Discount', 'City']),
        'order_by': [
            {
                'column': guids_from_titles(result_schema, ['Profit'])[0],
                'direction': 'DESC',
            }
        ],
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)

    assert r.status_code == 200
    data = r.json['result']['data']['Data']
    assert len(data[0]) == 2


def test_any_db_result(any_db_saved_connection, api_v1, data_api_all_v, any_db_table):
    data_api = data_api_all_v
    connection_id = any_db_saved_connection.uuid
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=connection_id,
        **data_source_settings_from_table(table=any_db_table),
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()

    ds.result_schema['sum'] = ds.field(formula='SUM([int_value])')
    ds.result_schema['max'] = ds.field(formula='MAX([int_value])')
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    ds = api_v1.save_dataset(ds).dataset

    # Just make sure that we can build the query
    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='string_value'),
            ds.find_field(title='sum'),
        ],
        order_by=[
            ds.find_field(title='string_value'),
            ds.find_field(title='sum'),
        ],
        filters=[
            ds.find_field(title='max').filter(op=WhereClauseOperation.LT, values=['8']),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json

    data_rows = get_data_rows(result_resp)
    assert len(data_rows) == 8


def test_order_of_fields_in_order_by_clause(api_v1, data_api_v1, clickhouse_table, connection_id):
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=connection_id,
        **data_source_settings_from_table(table=clickhouse_table),
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds.result_schema['pos'] = ds.field(formula='ANY([int_value])')
    ds.result_schema['neg'] = ds.field(formula='-ANY([int_value])')
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    ds = api_v1.save_dataset(ds).dataset

    def get_data(order_by_titles: List[str]) -> List[List[str]]:
        result_resp = data_api_v1.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title='int_value'),
            ],
            group_by=[
                ds.find_field(title='string_value'),
            ],
            order_by=[
                ds.find_field(title=title)
                for title in order_by_titles
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        result_data = result_resp.data
        return result_data['Data']

    # ORDER BY [pos], [neg] -> should be ascending
    pos_neg_data = get_data(['pos', 'neg'])
    assert int(pos_neg_data[0][0]) < int(pos_neg_data[1][0]) < int(pos_neg_data[2][0])

    # ORDER BY [neg], [pos] -> should be descending
    pos_neg_data = get_data(['neg', 'pos'])
    assert int(pos_neg_data[0][0]) > int(pos_neg_data[1][0]) > int(pos_neg_data[2][0])


def test_ch_parameterized_function_in_query(api_v1, data_api_v1, dataset_id):
    # Quantiles use ParameterizedFunction
    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset
    ds.result_schema['Quant'] = ds.field(formula='QUANTILE([Sales], 0.5)')
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(ds).dataset

    result_resp = data_api_v1.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Order Date'),
            ds.find_field(title='Quant'),
        ],
        group_by=[
            ds.find_field(title='Order Date'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK
    result_data = result_resp.data
    data_rows = result_data['Data']
    assert len(data_rows) > 1


def test_empty_query(api_v1, data_api_v1, dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset
    result_resp = data_api_v1.get_result(dataset=ds, fields=[], group_by=[], fail_ok=True)
    assert result_resp.status_code == HTTPStatus.BAD_REQUEST
    assert result_resp.bi_status_code == 'ERR.DS_API.EMPTY_QUERY'


def test_join_optimization(connection_id, api_v1, data_api_v1, two_clickhouse_tables_w_const_columns):
    table_1, table_2 = two_clickhouse_tables_w_const_columns
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
        ds.col('const') == ds.col('const')
    )
    ds.result_schema['cnt'] = ds.field(formula='COUNT()')
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(ds).dataset

    result_resp = data_api_v1.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='id'),
            ds.find_field(title='cnt'),
        ],
        group_by=[
            ds.find_field(title='id'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK
    result_data = result_resp.data
    data_rows = result_data['Data']
    assert len(data_rows) == 10
    for row in data_rows:
        assert row[1] == '1'  # If a JOIN is added, then the value will be 10


def test_duplicate_order_by(api_v1, data_api_v1, dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset

    field = ds.find_field(title='Sales')

    # ASC
    result_resp = data_api_v1.get_result(
        dataset=ds, fields=[field], group_by=[field],
        order_by=[field, field.desc],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK
    result_data = result_resp.data
    data_rows = result_data['Data']
    values = [float(row[0]) for row in data_rows]
    assert values == sorted(values)

    # DESC
    result_resp = data_api_v1.get_result(
        dataset=ds, fields=[field], group_by=[field],
        order_by=[field.desc, field],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK
    result_data = result_resp.data
    data_rows = result_data['Data']
    values = [float(row[0]) for row in data_rows]
    assert values == sorted(values, reverse=True)


def _check_median_for_dataset(ds: Dataset, api_v1: SyncHttpDatasetApiV1, data_api_v1: SyncHttpDataApiV1):
    orig_field_name = 'int_value'
    ds.result_schema['New Dim'] = ds.field(formula=f'[{orig_field_name}] % 3')
    ds.result_schema['Median'] = ds.field(formula=f'MEDIAN([{orig_field_name}])')
    ds.result_schema['Another Agg'] = ds.field(formula=f'AVG([{orig_field_name}])')
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset

    result_resp = data_api_v1.get_result(
        dataset=ds,
        fields=[
            ds.result_schema['New Dim'],
            ds.result_schema['Median'],
            ds.result_schema['Another Agg'],
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK


def _test_db_median(api_v1, data_api_v1, db_table, connection_id):
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=connection_id,
        **data_source_settings_from_table(table=db_table),
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset
    _check_median_for_dataset(ds, api_v1, data_api_v1)


def test_pg_median(api_v1, data_api_v1, postgres_table_fresh, pg_fresh_connection_id):
    _test_db_median(api_v1, data_api_v1, postgres_table_fresh, pg_fresh_connection_id)


def test_oracle_median(api_v1, data_api_v1, oracle_table, oracle_connection_id):
    _test_db_median(api_v1, data_api_v1, oracle_table, oracle_connection_id)


def test_greenplum_dataset_result(client, api_v1, data_api_v1, postgres_table, greenplum_connection_id):
    ds = Dataset()
    data_source_settings = data_source_settings_from_table(table=postgres_table)
    data_source_settings.update(
        source_type=SOURCE_TYPE_GP_TABLE,
        connection_id=greenplum_connection_id,
    )
    ds.sources['source_1'] = ds.source(**data_source_settings)
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds_resp = api_v1.save_dataset(dataset=ds)
    assert ds_resp.status_code == 200
    ds = ds_resp.dataset

    dataset_id = ds.id

    result_schema = get_result_schema(client, dataset_id)

    req_data = {
        'updates': [],
        'columns': [x['guid'] for x in result_schema],
        'group_by': [result_schema[i]['guid'] for i in range(1, len(result_schema))],
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)

    assert r.status_code == 200


def test_get_dataset_version_result_array_fields(client, api_v1, data_api_v1, connection_id):
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        source_type=SOURCE_TYPE_CH_TABLE,
        connection_id=connection_id,
        parameters=dict(
            db_name='test_data',
            table_name='t_arrays',
        )
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds_resp = api_v1.save_dataset(dataset=ds)
    assert ds_resp.status_code == 200
    ds = ds_resp.dataset
    dataset_id = ds.id

    result_schema = get_result_schema(client, dataset_id)
    # title_to_id = {field['title']: field['guid'] for field in result_schema}

    req_data = {
        'columns': guids_from_titles(result_schema, ['descr', 'arr_int', 'arr_float', 'arr_str']),
    }

    r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=req_data)
    assert r.json['result']['data']['Data']
    assert r.json['result']['fields']


def test_avatar_only_in_filters(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = api_v1.load_dataset(dataset=Dataset(id=dataset_id)).dataset
    ds.result_schema['Count'] = ds.field(formula='COUNT()')
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(ds).dataset

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='Count'),
        ],
        filters=[
            ds.find_field(title='City').filter(op=WhereClauseOperation.STARTSWITH, values=['New']),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK
    data_rows = get_data_rows(result_resp)
    assert len(data_rows) == 1


def test_duplicate_expressions(
        api_v1, data_api_v2,
        any_db_table, any_db_saved_connection,
):
    data_api = data_api_v2
    connection_id = any_db_saved_connection.uuid
    db_table = any_db_table

    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=connection_id,
        **data_source_settings_from_table(table=db_table),
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds.result_schema['Measure 1'] = ds.field(formula='SUM([int_value])')
    ds.result_schema['Measure 2'] = ds.field(formula='SUM([int_value])')  # yes, they are identical
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(dataset=ds).dataset

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='int_value'),
            ds.find_field(title='Measure 1'),
            ds.find_field(title='Measure 2'),
        ],
        order_by=[
            ds.find_field(title='Measure 1'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK
    data_rows = get_data_rows(result_resp)
    assert len(data_rows) > 1

    for row in data_rows:
        assert row[1] == row[2]
