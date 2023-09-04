""" ... """
# pylint: disable=redefined-outer-name

from __future__ import annotations

import json
import random
import copy
import math
import uuid
from http import HTTPStatus

import shortuuid

from bi_connector_bundle_ch_frozen.ch_frozen_demo.core.constants import CONNECTION_TYPE_CH_FROZEN_DEMO
from bi_constants.enums import (
    AggregationFunction,
    BIType,
    CreateDSFrom,
    CalcMode,
)

from bi_api_lib.enums import DatasetAction
from bi_api_client.dsmaker.primitives import Dataset
from bi_api_client.dsmaker.api.data_api import HttpDataApiResponse
from bi_testing.utils import get_log_record, guids_from_titles

from bi_core.exc import USObjectNotFoundException

from bi_connector_bundle_ch_frozen.ch_frozen_base.core.constants import (
    SOURCE_TYPE_CH_FROZEN_SOURCE, SOURCE_TYPE_CH_FROZEN_SUBSELECT,
)

from bi_connector_mysql.core.constants import SOURCE_TYPE_MYSQL_SUBSELECT
from bi_connector_oracle.core.constants import SOURCE_TYPE_ORACLE_TABLE
from bi_connector_greenplum.core.constants import SOURCE_TYPE_GP_TABLE
from bi_connector_postgresql.core.postgresql.constants import SOURCE_TYPE_PG_TABLE, SOURCE_TYPE_PG_SUBSELECT

from bi_api_lib_tests.utils import get_result_schema, replace_dataset_connection as replace_connection


def _get_index_for_type(dataset: Dataset, type: BIType) -> int:
    i = 0
    for i, field in enumerate(dataset.result_schema):
        if field.cast == type:
            break
    return i


def test_get_preview_immediately(client, api_v1, data_api_v1, connection_id):
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        source_type=CreateDSFrom.CH_TABLE,
        connection_id=connection_id,
        parameters=dict(
            db_name='test_data',
            table_name='sample_superstore',
        )
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds_resp = api_v1.save_dataset(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    dataset_id = ds.id

    preview_resp = data_api_v1.get_response_for_dataset_preview(
        dataset_id=dataset_id,
    )
    assert preview_resp.status_code == HTTPStatus.OK, preview_resp.json


def test_create_dataset_from_oracle(client, api_v1, oracle_connection_id, oracle_table):
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        source_type=SOURCE_TYPE_ORACLE_TABLE,
        connection_id=oracle_connection_id,
        parameters=dict(
            table_name=oracle_table.name,
        )
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds_resp = api_v1.save_dataset(dataset=ds)
    assert ds_resp.status_code == 200
    ds = ds_resp.dataset
    dataset_id = ds.id
    client.delete('/api/v1/datasets/{}'.format(dataset_id))


def test_create_pg_dataset_in_custom_us_dir(client, api_v1, pg_connection_id, default_sync_usm_per_test):
    usm = default_sync_usm_per_test

    try:
        # clean up in case this test failed previously
        entry = usm.get_raw_entry_by_key('special_dir/my_dataset')
        client.delete('/api/v1/datasets/{}'.format(entry['entryId']))
    except USObjectNotFoundException:
        pass

    ds = Dataset(name='my_dataset')
    ds.sources['source_1'] = ds.source(
        source_type=SOURCE_TYPE_PG_TABLE,
        connection_id=pg_connection_id,
        parameters=dict(
            table_name='supersample',
        )
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds_resp = api_v1.save_dataset(dataset=ds, dir_path='special_dir')
    assert ds_resp.status_code == 200
    ds = ds_resp.dataset
    dataset_id = ds.id

    entry = usm.get_raw_entry_by_key('special_dir/my_dataset')
    assert entry['entryId'] == dataset_id

    client.delete('/api/v1/datasets/{}'.format(dataset_id))


def test_create_entity_with_existing_name(client, api_v1, connection_id):
    name = 'double_test__{}'.format(random.randint(0, 10000000))

    first_ds = Dataset(name=name)
    first_ds.sources['source_1'] = first_ds.source(
        source_type=CreateDSFrom.CH_TABLE,
        connection_id=connection_id,
        parameters=dict(
            db_name='test_data',
            table_name='SampleLite',
        )
    )
    first_ds.source_avatars['avatar_1'] = first_ds.sources['source_1'].avatar()
    first_ds = api_v1.apply_updates(dataset=first_ds).dataset
    ds_resp = api_v1.save_dataset(dataset=first_ds)
    assert ds_resp.status_code == 200
    first_ds = ds_resp.dataset

    second_ds = Dataset(name=name)
    second_ds.sources['source_1'] = second_ds.source(
        source_type=CreateDSFrom.CH_TABLE,
        connection_id=connection_id,
        parameters=dict(
            db_name='test_data',
            table_name='SampleLite',
        )
    )
    second_ds.source_avatars['avatar_1'] = second_ds.sources['source_1'].avatar()
    second_ds = api_v1.apply_updates(dataset=second_ds, fail_ok=True).dataset
    ds_resp = api_v1.save_dataset(dataset=second_ds, fail_ok=True)
    assert ds_resp.status_code == 400
    assert ds_resp.json['message'] == 'The entry already exists'
    assert ds_resp.json['code'] == 'ERR.DS_API.US.BAD_REQUEST.ALREADY_EXISTS'

    client.delete('/api/v1/datasets/{}'.format(first_ds.id))


def test_get_dataset_with_deleted_connection(api_v1, client, dataset_id, connection_id):
    r = client.get('/api/v1/datasets/{}/versions/draft'.format(dataset_id))
    assert r.status_code == 200
    orig_data = r.json

    r = client.delete('/api/v1/connections/{}'.format(connection_id))
    assert r.status_code == 200

    r = client.get('/api/v1/datasets/{}/versions/draft'.format(dataset_id))
    assert r.status_code == 200
    new_data = r.json
    assert orig_data['dataset']['result_schema'] == new_data['dataset']['result_schema']
    assert orig_data['dataset']['sources'] == new_data['dataset']['sources']
    assert orig_data['dataset']['source_avatars'] == new_data['dataset']['source_avatars']
    # Missing connection is assumed to be ClickHouse, so nothing really changes in options
    orig_data['options'].pop('supported_functions')
    new_data['options'].pop('supported_functions')
    assert orig_data['options'] == new_data['options']


def test_get_one_dataset(client, dataset_id):
    ds = client.get(
        '/api/v1/datasets/{}/versions/draft'.format(dataset_id)
    ).json
    assert ds['is_favorite'] is False
    assert ds['permissions'] == {'admin': True, 'edit': True, 'execute': True, 'read': True}


def test_get_dataset_fields(client, data_api_v1, dataset_id):
    def _check_fields(ds_fields):
        assert isinstance(ds_fields, list)
        assert all(f in (x['title'] for x in ds_fields) for f in ['Category', 'City', 'Country'])
        assert all(f['calc_mode'] in {item.value for item in CalcMode} for f in ds_fields)

    r = client.get('/api/v1/datasets/{}/fields'.format(dataset_id)).json
    assert 'revision_id' not in r  # revision_id should only be presented in async version
    _check_fields(r['fields'])

    r = data_api_v1.get_fields(dataset_id)
    assert r.json['revision_id'] is None
    _check_fields(r.json['fields'])


def test_dataset_revision_id(caplog, client, data_api_v1, dataset_id):
    caplog.set_level('WARNING')

    r = data_api_v1.get_fields(dataset_id)
    assert r.json['revision_id'] is None

    dataset = client.get(
        '/api/v1/datasets/{}/versions/draft'.format(dataset_id)
    ).json['dataset']
    r = client.put(
        '/api/v1/datasets/{}/versions/draft'.format(dataset_id),
        data=json.dumps({'dataset': dataset}),
        content_type='application/json'
    )  # generates a revision_id for the dataset
    assert r.status_code == 200
    r = data_api_v1.get_fields(dataset_id)
    revision_id = r.json['revision_id']
    assert isinstance(revision_id, str)
    result_schema = copy.deepcopy(dataset['result_schema'])

    def _check_revision_id_mismatch_warning(request_revision_id, actual_revision_id, expected):
        raw_body = {'columns': guids_from_titles(result_schema, ['Discount', 'City'])}
        if request_revision_id:
            raw_body['revision_id'] = request_revision_id

        r = data_api_v1.get_response_for_dataset_result(dataset_id=dataset_id, raw_body=raw_body)
        assert r.status_code == 200
        log_record = get_log_record(caplog, predicate=lambda r: r.funcName == 'check_dataset_revision_id')
        if expected:
            warning = f'Dataset revision id mismatch: got {request_revision_id} from the request, ' \
                      f'but found {actual_revision_id} in the current dataset'
            assert len(log_record) == 1
            assert log_record[0].msg == warning
        else:
            assert not log_record
        caplog.clear()

    _check_revision_id_mismatch_warning('lol', revision_id, True)  # incorrect revision id - log warning
    _check_revision_id_mismatch_warning(revision_id, revision_id, False)  # correct revision id - no warning
    _check_revision_id_mismatch_warning(None, revision_id, False)  # no revision id from request - also no warning


def test_create_dataset(client, dataset_id):
    dataset_version = client.get(
        '/api/v1/datasets/{}/versions/draft'.format(dataset_id)
    ).json

    assert len(
        dataset_version['dataset']['sources'][0]['raw_schema']
    ) == len(
        dataset_version['dataset']['result_schema']
    )
    assert all(
        dataset_version['dataset']['sources'][0]['raw_schema'][i]['name'] ==
        dataset_version['dataset']['result_schema'][i]['title']
        for i in range(len(dataset_version['dataset']['result_schema']))
    )
    assert all(item['calc_mode'] == 'direct' for item in dataset_version['dataset']['result_schema'])


def test_update_dataset_version(client, dataset_id):
    dataset_version = client.get(
        '/api/v1/datasets/{}/versions/draft'.format(dataset_id)
    ).json

    result_schema = copy.deepcopy(dataset_version['dataset']['result_schema'])
    result_schema.append(dict(result_schema[1]))
    del result_schema[-1]['guid']
    result_schema[-1]['aggregation'] = 'sum'
    result_schema[-1]['source'] = result_schema[1]['title']
    result_schema[-1]['title'] += '_copy'
    result_schema[-1]['title'] = 'New Field'
    r = client.put(
        '/api/v1/datasets/{}/versions/draft'.format(dataset_id),
        data=json.dumps({'dataset': {'result_schema': result_schema}}),
        content_type='application/json'
    )
    assert r.status_code == 200

    updated_dataset_version = client.get(
        '/api/v1/datasets/{}/versions/draft'.format(dataset_id)
    ).json

    assert len(
        updated_dataset_version['dataset']['result_schema']
    ) == len(
        dataset_version['dataset']['result_schema']
    ) + 1

    guids = [x['guid'] for x in updated_dataset_version['dataset']['result_schema']]
    assert len(guids) == len(set(guids))

    assert updated_dataset_version['dataset']['result_schema'][-1]['aggregation'] == 'sum'


def test_update_dataset_with_invalid_formulas(client, dataset_id):
    dataset_version = client.get(
        '/api/v1/datasets/{}/versions/draft'.format(dataset_id)
    ).json

    result_schema = copy.deepcopy(dataset_version['dataset']['result_schema'])
    result_schema.append(dict(result_schema[1]))
    result_schema.append(dict(result_schema[1]))
    result_schema[-2]['title'] = 'New'
    result_schema[-2]['guid'] = str(uuid.uuid4())
    result_schema[-2]['calc_mode'] = 'formula'
    result_schema[-2]['formula'] = '[Invalid Field] + [Another Invalid Field]'
    result_schema[-1]['title'] = 'New 2'
    result_schema[-1]['guid'] = str(uuid.uuid4())
    result_schema[-1]['calc_mode'] = 'formula'
    result_schema[-1]['formula'] = 'sum({})'.format(result_schema[0]['title'])
    result_schema[-1]['aggregation'] = 'sum'

    r = client.put(
        '/api/v1/datasets/{}/versions/draft'.format(dataset_id),
        data=json.dumps({'dataset': {'result_schema': result_schema}}),
        content_type='application/json'
    )
    assert r.status_code == 200
    dataset_version = client.get(
        '/api/v1/datasets/{}/versions/draft'.format(dataset_id)
    ).json
    result_schema = dataset_version['dataset']['result_schema']
    assert result_schema[-2]['formula'] == '[Invalid Field] + [Another Invalid Field]'
    assert result_schema[-1]['formula'] == 'sum({})'.format(result_schema[0]['title'])


def test_get_preview(client, data_api_v1, static_dataset_id):
    dataset_id = static_dataset_id
    ds_draft = client.get(
        '/api/v1/datasets/{}/versions/draft'.format(dataset_id)
    ).json

    ds_draft['dataset']['result_schema'][0]['hidden'] = True

    preview = data_api_v1.get_response_for_dataset_preview(
        dataset_id=dataset_id,
        raw_body={'limit': 13},
    ).json

    assert set(preview['result'].keys()) == {'data', 'data_export_forbidden'}
    assert set(preview['result']['data'].keys()) == {'Type', 'Data'}
    assert isinstance(preview['result']['data']['Data'], list)
    assert len(preview['result']['data']['Data']) == 13


def test_get_preview_with_sum_of_ints(api_v1, data_api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    i = _get_index_for_type(ds, BIType.integer)
    ds = api_v1.apply_updates(dataset=ds, updates=[
        ds.result_schema[i].update(aggregaion=AggregationFunction.sum.name)
    ]).dataset

    preview_data = data_api_v1.get_preview(dataset=ds).data
    assert '.' not in preview_data['Data'][0][i]


def test_delete_dataset(client, dataset_id):
    response = client.delete(
        '/api/v1/datasets/{}'.format(dataset_id),
    )
    assert response.status_code == 200


def test_aggregation_sum(api_v1, data_api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    old_len = len(ds.result_schema)
    i = _get_index_for_type(ds, BIType.float)
    ds.result_schema['new_field'] = ds.field(
        avatar_id=ds.source_avatars[0].id,
        source=ds.result_schema[i].title,
        aggregation=AggregationFunction.sum,
        title='Sum of {}'.format(ds.result_schema[i].title)
    )
    ds = api_v1.apply_updates(dataset=ds).dataset
    assert len(ds.result_schema) == old_len + 1
    new_i = i + 1  # New field is added at the beginning, so the ith is moved up by 1, and the new field is at 0
    preview_resp = data_api_v1.get_preview(dataset=ds)
    assert preview_resp.status_code == 200
    preview_data = preview_resp.data
    assert math.fabs(
        float(preview_data['Data'][0][0]) -
        float(preview_data['Data'][0][new_i])
    ) < 1e-6


def test_simple_formula(api_v1, data_api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    i = _get_index_for_type(ds, BIType.float)
    field = ds.result_schema[i]
    formula = f'[{field.title}] * 2'
    ds.result_schema[f'Doubled {field.title}'] = ds.field(formula=formula)
    ds_resp = api_v1.apply_updates(dataset=ds)
    ds = ds_resp.dataset

    resp_result_schema = ds_resp.json['dataset']['result_schema']
    assert resp_result_schema[0]['calc_mode'] == 'formula'
    assert resp_result_schema[0]['formula'] == formula
    assert not resp_result_schema[0]['autoaggregated']
    assert resp_result_schema[0]['aggregation_locked']

    new_i = i + 1
    preview_resp = data_api_v1.get_preview(dataset=ds)
    assert preview_resp.status_code == 200
    preview_data = preview_resp.data
    assert all(
        math.fabs(float(row[new_i]) * 2 - float(row[0])) < 1e-5
        for row in preview_data['Data']
    )

    ds.result_schema['Agg Formula'] = ds.field(formula=f'MAX([{field.title}])')
    ds_resp = api_v1.apply_updates(dataset=ds)

    resp_result_schema = ds_resp.json['dataset']['result_schema']
    assert resp_result_schema[0]['calc_mode'] == 'formula'
    assert resp_result_schema[0]['autoaggregated']
    assert resp_result_schema[0]['aggregation_locked']


def test_formula_with_aggregated_fields(api_v1, data_api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    ds.result_schema['Customer Count'] = ds.field(
        avatar_id=ds.source_avatars[0].id,
        source='Customer ID',
        aggregation=AggregationFunction.countunique,
    )
    ds.result_schema['QuanPerCust'] = ds.field(
        formula='SUM([Quantity]) / [Customer Count]',
    )
    ds = api_v1.apply_updates(dataset=ds).dataset

    preview_resp = data_api_v1.get_preview(dataset=ds)
    assert preview_resp.status_code == 200


def test_formula_with_string(api_v1, data_api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    ds.result_schema['Test'] = ds.field(
        formula="[Order ID]in ('US-2017-158946')",
        cast=BIType.integer,
    )
    ds = api_v1.apply_updates(dataset=ds).dataset
    preview_resp = data_api_v1.get_preview(dataset=ds)
    assert preview_resp.status_code == 200


def test_aggregation_on_formula(api_v1, data_api_v1, static_dataset_id):
    ds = api_v1.load_dataset(dataset=Dataset(id=static_dataset_id)).dataset
    i = _get_index_for_type(ds, BIType.float)
    field = ds.result_schema[i]
    ds.result_schema[f'Doubled {field.title}'] = ds.field(
        formula=f'[{field.title}] * 2',
        aggregation=AggregationFunction.sum,
    )
    ds.result_schema['Group Size'] = ds.field(
        formula='COUNT()',
    )
    ds = api_v1.apply_updates(dataset=ds).dataset
    new_i = i + 2
    preview_resp = data_api_v1.get_preview(dataset=ds)
    assert preview_resp.status_code == 200
    preview_data = preview_resp.data
    assert all(
        math.fabs(float(item[new_i]) * 2 * float(item[0]) - float(item[1])) < 1e-6
        for item in preview_data['Data']
    )


def test_create_dataset_with_null_date(client, api_v1, connection_id):
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        source_type=CreateDSFrom.CH_TABLE,
        connection_id=connection_id,
        parameters=dict(
            db_name='test_data',
            table_name='null_date',
        )
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds_resp = api_v1.save_dataset(dataset=ds)
    assert ds_resp.status_code == 200
    ds = ds_resp.dataset
    dataset_id = ds.id

    client.delete('/api/v1/datasets/{}'.format(dataset_id))


def test_create_dataset_with_uuid(client, api_v1, connection_id):
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        source_type=CreateDSFrom.CH_TABLE,
        connection_id=connection_id,
        parameters=dict(
            db_name='test_data',
            table_name='t_uuid',
        )
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds_resp = api_v1.save_dataset(dataset=ds)
    assert ds_resp.status_code == 200
    ds = ds_resp.dataset
    dataset_id = ds.id

    client.delete('/api/v1/datasets/{}'.format(dataset_id))


def test_ch_subselect(request, client, api_v1, data_api_v1,  ch_subselectable_connection_id):
    conn_id = ch_subselectable_connection_id
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=conn_id,
        source_type=CreateDSFrom.CH_SUBSELECT,
        parameters=dict(
            subsql=r"""
                select
                    arrayJoin(range(7)) as a,
                    ':b\r' as b,
                    cast('2020-01-01 00:00:00' as DateTime('Pacific/Chatham')) as dttzfld,
                    cast('2020-01-01 00:00:00' as DateTime) as dtfld

            """,
        ))
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds.result_schema['dttzfrm'] = ds.field(formula="DATETIMETZ([dttzfld], 'Pacific/Chatham')")
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(dataset=ds, preview=False).dataset
    # Get the value through the US:
    ds = api_v1.load_dataset(dataset=ds).dataset

    dt_field = ds.sources['source_1'].raw_schema[2]
    assert dt_field.native_type['timezone_name'] == 'Pacific/Chatham'

    ds_id = ds.id

    def teardown(ds_id=ds_id):
        client.delete('/api/v1/datasets/{}'.format(ds_id))

    request.addfinalizer(teardown)

    result_schema = get_result_schema(client, ds_id)

    guids = {fld['title']: fld['guid'] for fld in result_schema}
    resp = data_api_v1.get_response_for_dataset_result(
        dataset_id=ds_id,
        raw_body={
            'columns': [fld['guid'] for fld in result_schema],
            'where': [
                {
                    'column': guids['dttzfrm'],
                    'operation': 'BETWEEN',
                    'values': [
                        '2019-12-30T00:00:00.000Z',
                        '2021-04-15T23:59:59.999Z',
                    ],
                },
            ],
            'limit': 3,
        },
    )
    rd = resp.json
    assert resp.status_code == 200, rd
    assert rd['result']['data']['Data'][0][2] == ':b\r', rd


def test_ch_disabled_subselect(request, client, api_v1, data_api_v1, static_connection_id):
    conn_id = static_connection_id
    ds = Dataset()
    # Disallowing in the 'get_sql_source' level,
    # so an attempt to retrieve the schema will also fail.
    ds.sources['source_1'] = ds.source(
        connection_id=conn_id,
        source_type=CreateDSFrom.CH_SUBSELECT,
        parameters=dict(
            subsql="select arrayJoin(range(7)) as a, '2' as b",
        ))
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds, fail_ok=True).dataset
    ds = api_v1.save_dataset(dataset=ds, preview=False).dataset

    ds_id = ds.id

    def teardown(ds_id=ds_id):
        client.delete('/api/v1/datasets/{}'.format(ds_id))

    request.addfinalizer(teardown)

    result_schema = get_result_schema(client, ds_id)
    assert not result_schema, 'dataset should be with no schema (and with errors)'
    assert ds.component_errors.items, 'dataset should be with errors (and no schema)'


def test_pg_subselect(request, client, api_v1, data_api_v1, pg_subselectable_connection_id):
    conn_id = pg_subselectable_connection_id
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=conn_id,
        source_type=SOURCE_TYPE_PG_SUBSELECT,
        parameters=dict(
            subsql=r"""
                select
                    unnest(ARRAY[0, 1, 3, 5, 7]) as "a a",
                    '2'::text as b,  -- without cast, returns as '705: unknown' in cursor
                    3 as c
            """,
        ))
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(dataset=ds, preview=False).dataset

    ds_id = ds.id

    def teardown(ds_id=ds_id):
        client.delete('/api/v1/datasets/{}'.format(ds_id))

    request.addfinalizer(teardown)

    result_schema = get_result_schema(client, ds_id)

    colnames = [col['title'] for col in result_schema]
    assert colnames == ['a a', 'b', 'c']

    columns = [result_schema[0]['guid']]
    response = data_api_v1.get_response_for_dataset_result(
        dataset_id=ds_id,
        raw_body={'columns': columns, 'limit': 3},
    )
    assert response.status_code == 200
    values = {row[0] for row in response.json['result']['data']['Data']}
    assert len(values) == 3  # because 'limit'
    assert values.issubset({'0', '1', '3', '5', '7'})


def test_mysql_subselect(request, client, api_v1, data_api_v1, mysql_connection_id):
    conn_id = mysql_connection_id
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=conn_id,
        source_type=SOURCE_TYPE_MYSQL_SUBSELECT,
        parameters=dict(
            subsql="select concat(0, '%') as value",
        ))
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(dataset=ds, preview=False).dataset

    ds_id = ds.id

    def teardown(ds_id=ds_id):
        client.delete('/api/v1/datasets/{}'.format(ds_id))

    request.addfinalizer(teardown)

    preview_resp = data_api_v1.get_preview(dataset=ds)
    assert preview_resp.status_code == 200
    preview_data = preview_resp.data['Data']
    assert len(preview_data) == 1
    assert preview_data[0][0] == '0%'


def test_pg_invalid_subselect(request, client, api_v1, data_api_v1, pg_subselectable_connection_id):
    conn_id = pg_subselectable_connection_id
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=conn_id,
        source_type=SOURCE_TYPE_PG_SUBSELECT,
        parameters=dict(
            subsql=r"select 1 as a;",
        ))
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds, fail_ok=True).dataset
    assert len(ds.component_errors.items) == 1, ds.component_errors.items
    cerrs = ds.component_errors.items[0]
    assert len(cerrs.errors) == 1
    cerr = cerrs.errors[0]
    assert cerr.code.startswith('ERR.DS_API.DB')
    assert cerr.details['db_message']
    assert cerr.details['query']


def test_pg_unsupported_type(request, client, api_v1, data_api_v1, pg_subselectable_connection_id):
    conn_id = pg_subselectable_connection_id
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=conn_id,
        source_type=SOURCE_TYPE_PG_SUBSELECT,
        parameters=dict(
            subsql=r"""
                select
                    0 as int,
                    '1' as not_string,
                    2::oid as not_int
            """,
        ))
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(dataset=ds, preview=False).dataset

    ds_id = ds.id

    def teardown(ds_id=ds_id):
        client.delete('/api/v1/datasets/{}'.format(ds_id))

    request.addfinalizer(teardown)

    result_schema = get_result_schema(client, ds_id)

    colnames = [col['title'] for col in result_schema]
    assert colnames == ['int', 'not_string', 'not_int']
    assert result_schema[1]['data_type'] == 'unsupported'
    assert result_schema[2]['data_type'] == 'unsupported'

    response = data_api_v1.get_response_for_dataset_result(
        dataset_id=ds_id,
        raw_body={
            "columns": [result_schema[0]['guid']],
            "limit": 3,
        },
    )
    assert response.status_code == 200

    response = data_api_v1.get_response_for_dataset_result(
        dataset_id=ds_id,
        raw_body={
            "columns": [result_schema[0]['guid'], result_schema[1]['guid']],
            "limit": 3,
        },
    )

    assert response.status_code == 400
    assert 'Cannot select fields of unsupported' in response.json['message']


def _get_result(data_api_v1, the_ds, fail_ok) -> HttpDataApiResponse:
    return data_api_v1.get_result(
        dataset=the_ds,
        fields=[the_ds.find_field(title='City')],
        order_by=[the_ds.find_field(title='City')],
        fail_ok=fail_ok,
    )


def test_dataset_copy_direct(api_v1, data_api_v1, dataset_id):
    original_ds = api_v1.load_dataset(Dataset(id=dataset_id)).dataset
    original_result = _get_result(data_api_v1, original_ds, fail_ok=False).data

    copy_ds = api_v1.copy_dataset(
        original_ds, new_key=f'ds_copy/{original_ds.id}_copy_{shortuuid.uuid()}'
    ).dataset
    copy_result = _get_result(data_api_v1, copy_ds, fail_ok=False).data

    # Ensure result is the same
    assert copy_result == original_result


def test_dataset_with_deleted_connection(api_v1, data_api_v1, dataset_id, connection_id, default_sync_usm):
    ds = api_v1.load_dataset(Dataset(id=dataset_id)).dataset
    default_sync_usm.delete(default_sync_usm.get_by_id(connection_id))

    result_resp = _get_result(data_api_v1, ds, fail_ok=True)

    assert result_resp.status_code == 400
    assert result_resp.json['code'] == 'ERR.DS_API.REFERENCED_ENTRY_NOT_FOUND'
    assert result_resp.json['message'] == f"Referenced connection {connection_id} was deleted"


def test_dataset_created_via(api_v1, connection_id, default_sync_usm_per_test):
    def _prepare_ch_ds():
        _ds = Dataset()
        _ds.sources['source_1'] = _ds.source(
            source_type=CreateDSFrom.CH_TABLE,
            connection_id=connection_id,
            parameters=dict(
                db_name='test_data',
                table_name='sample_superstore',
            )
        )
        _ds.source_avatars['avatar_1'] = _ds.sources['source_1'].avatar()
        return api_v1.apply_updates(dataset=_ds).dataset

    usm = default_sync_usm_per_test

    ds = _prepare_ch_ds()
    ds_resp = api_v1.save_dataset(dataset=ds)
    assert ds_resp.status_code == 200
    ds = ds_resp.dataset

    ds_us_entry = usm.get_raw_entry(ds.id)
    assert ds_us_entry['meta']['created_via'] == 'user'

    ds_yt_to_dl = _prepare_ch_ds()
    ds_yt_to_dl_resp = api_v1.save_dataset(dataset=ds_yt_to_dl, created_via='yt_to_dl')
    assert ds_yt_to_dl_resp.status_code == 200
    ds_yt_to_dl = ds_yt_to_dl_resp.dataset

    ds_yt_to_dl_us_entry = usm.get_raw_entry(ds_yt_to_dl.id)
    assert ds_yt_to_dl_us_entry['meta']['created_via'] == 'yt_to_dl'


# TODO FIX: Make class test and parametrize at least for PG and CH
def test_index_fetching_pg(api_v1, pg_connection_id, postgres_db):
    table_name = f"test_tbl_{shortuuid.uuid()}"
    idx_1_name = f"{table_name}_idx_1"
    idx_2_name = f"{table_name}_idx_2"
    source_alias = "idxed_source"
    avatar_alias = "avatar_1"

    postgres_db.execute(f"""CREATE TABLE "{table_name}" (num_1 integer, num_2 integer, txt text)""")
    postgres_db.execute(f"""CREATE INDEX "{idx_1_name}" ON "{table_name}" (num_1)""")

    ds = Dataset()
    ds.sources[source_alias] = ds.source(
        source_type=SOURCE_TYPE_PG_TABLE,
        connection_id=pg_connection_id,
        parameters=dict(
            table_name=table_name,
        )
    )
    ds.source_avatars[avatar_alias] = ds.sources[source_alias].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset

    # Check that validation returns indexes
    assert ds.sources[source_alias].index_info_set == [dict(columns=['num_1'], kind=None)]
    source_id = ds.sources[source_alias].id

    ds_resp = api_v1.save_dataset(dataset=ds)
    assert ds_resp.status_code == 200

    # Reloading dataset and check that indexes was saved
    ds = api_v1.load_dataset(Dataset(id=ds_resp.dataset.id)).dataset
    assert ds.sources[source_id].index_info_set == [dict(columns=['num_1'], kind=None)]

    # Modifying index
    postgres_db.execute(f"""CREATE INDEX "{idx_2_name}" ON "{table_name}" (num_1, num_2)""")
    expected_index_set = [
        dict(columns=['num_1'], kind=None),
        dict(columns=['num_1', 'num_2'], kind=None),
    ]
    # Ensure indexes set was updated
    ds = api_v1.apply_updates(ds, [ds.sources[source_id].refresh()]).dataset
    assert ds.sources[source_id].index_info_set == expected_index_set
    # Even after save
    api_v1.save_dataset(ds)
    ds = api_v1.load_dataset(ds).dataset
    assert ds.sources[source_id].index_info_set == expected_index_set

    # Dropping all indexes
    postgres_db.execute(f"""DROP INDEX "{idx_1_name}" """)
    postgres_db.execute(f"""DROP INDEX "{idx_2_name}" """)

    # Ensure indexes set was updated
    ds = api_v1.apply_updates(ds, [ds.sources[source_id].refresh()]).dataset
    assert ds.sources[source_id].index_info_set == []
    # Even after save
    api_v1.save_dataset(ds)
    ds = api_v1.load_dataset(ds).dataset
    assert ds.sources[source_id].index_info_set == []

    # Emulating unknown index config push
    ds.sources[source_id].index_info_set = None
    api_v1.save_dataset(ds)
    ds = api_v1.load_dataset(ds).dataset
    assert ds.sources[source_id].index_info_set is None


def test_create_dataset_from_greenplum(client, api_v1, greenplum_connection_id):
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        source_type=SOURCE_TYPE_GP_TABLE,
        connection_id=greenplum_connection_id,
        parameters=dict(
            table_name='supersample',
        )
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds_resp = api_v1.save_dataset(dataset=ds)
    assert ds_resp.status_code == 200
    ds = ds_resp.dataset
    dataset_id = ds.id

    client.delete('/api/v1/datasets/{}'.format(dataset_id))


def test_replace_connection_clickhouse_to_frozen_preconfigured_subselect_source(
        api_v1,
        default_sync_usm,
        connectors_settings,
        static_ch_frozen_demo_connection_id,
        ch_subselectable_connection_id,
):
    old_connection_id, new_connection_id = ch_subselectable_connection_id, static_ch_frozen_demo_connection_id
    frozen_settings = connectors_settings[CONNECTION_TYPE_CH_FROZEN_DEMO]

    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=old_connection_id,
        source_type=CreateDSFrom.CH_SUBSELECT,
        parameters=dict(
            subsql='select * from samples.SampleLite limit 10',
            title=frozen_settings.SUBSELECT_TEMPLATES[0]['title'],
        ),
    )

    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.json
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.json
    ds = ds_resp.dataset
    ds_resp = api_v1.save_dataset(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.json
    ds = ds_resp.dataset

    original_result_schema = ds.result_schema
    ds_resp = replace_connection(
        api_v1,
        ds=ds,
        old_connection_id=old_connection_id,
        new_connection_id=new_connection_id,
        fail_ok=True,
    )
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST, ds_resp.json
    ds = ds_resp.dataset

    assert ds.sources[0].source_type == SOURCE_TYPE_CH_FROZEN_SOURCE, ds.sources

    new_source_id = str(uuid.uuid4())
    db_name = 'samples'
    table_name = frozen_settings.SUBSELECT_TEMPLATES[0]['title']

    ds_resp = api_v1.apply_updates(dataset=ds, updates=[
        {
            'action': DatasetAction.add_source.name,
            'source': {
                'connection_id': new_connection_id, 'id': new_source_id,
                'source_type': SOURCE_TYPE_CH_FROZEN_SOURCE.name,
                'parameters': {'db_name': db_name, 'table_name': table_name}, 'title': f'{db_name}.{table_name}',
            },
        },
        {
            'action': DatasetAction.update_source_avatar.name,
            'source_avatar': {
                'id': ds.source_avatars['avatar_1'].id, 'source_id': new_source_id, 'title': f'{db_name}.{table_name}',
            },
        },
        {
            'action': DatasetAction.delete_source.name,
            'source': {'id': ds.sources['source_1'].id},
        },
    ])
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.json
    ds = ds_resp.dataset

    ds_resp = api_v1.apply_updates(dataset=ds, updates=[
        field.delete()
        for field in original_result_schema
    ])
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.json


def test_replace_connection_clickhouse_to_frozen_subselect_source(
        api_v1,
        default_sync_usm,
        connectors_settings,
        static_ch_frozen_demo_connection_id,
        ch_subselectable_connection_id,
):
    old_connection_id, new_connection_id = ch_subselectable_connection_id, static_ch_frozen_demo_connection_id
    frozen_settings = connectors_settings[CONNECTION_TYPE_CH_FROZEN_DEMO]

    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=old_connection_id,
        source_type=CreateDSFrom.CH_SUBSELECT,
        parameters=dict(
            subsql='select * from samples.SampleLite limit 10',
            title=frozen_settings.SUBSELECT_TEMPLATES[0]['title'],
        ),
    )

    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.json
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.json
    ds = ds_resp.dataset
    ds_resp = api_v1.save_dataset(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.json
    ds = ds_resp.dataset

    original_result_schema = ds.result_schema
    ds_resp = replace_connection(
        api_v1,
        ds=ds,
        old_connection_id=old_connection_id,
        new_connection_id=new_connection_id,
        fail_ok=True,
    )
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST, ds_resp.json
    ds = ds_resp.dataset

    assert ds.sources[0].source_type == SOURCE_TYPE_CH_FROZEN_SOURCE, ds.sources

    new_source_id = str(uuid.uuid4())
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[
        {
            'action': DatasetAction.add_source.name,
            'source': {
                'connection_id': new_connection_id, 'id': new_source_id,
                'source_type': SOURCE_TYPE_CH_FROZEN_SUBSELECT.name,
                'parameters': {'subsql': 'select * from samples.SampleLite'},
                'title': 'My SQL Source',
            },
        },
        {
            'action': DatasetAction.update_source_avatar.name,
            'source_avatar': {
                'id': ds.source_avatars['avatar_1'].id, 'source_id': new_source_id, 'title': 'My SQL Source',
            },
        },
        {
            'action': DatasetAction.delete_source.name,
            'source': {'id': ds.sources['source_1'].id},
        },
    ])
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.json
    ds = ds_resp.dataset

    ds_resp = api_v1.apply_updates(dataset=ds, updates=[
        field.delete()
        for field in original_result_schema
    ])
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.json


def test_create_dataset_ch_arrays(client, api_v1, connection_id):
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        source_type=CreateDSFrom.CH_TABLE,
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

    client.delete('/api/v1/datasets/{}'.format(dataset_id))
