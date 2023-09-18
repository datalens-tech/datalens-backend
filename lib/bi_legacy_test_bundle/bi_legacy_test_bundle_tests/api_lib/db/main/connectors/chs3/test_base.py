from __future__ import annotations

import json
from http import HTTPStatus

import pytest

from dl_api_client.dsmaker.primitives import Dataset
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_constants.enums import (
    BIType,
    CreateDSFrom,
    WhereClauseOperation,
    DataSourceRole,
    FileProcessingStatus,
)
from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from dl_connector_bundle_chs3.chs3_gsheets.core.constants import SOURCE_TYPE_GSHEETS_V2
from dl_connector_bundle_chs3.file.core.constants import SOURCE_TYPE_FILE_S3_TABLE
from dl_core.db import SchemaColumn


@pytest.fixture(
    scope='function',
    params=['file_connection_params', 'gsheets_v2_connection_params'],
    ids=['file', 'gsheets'],
)
def test_chs3_connection_params(request):
    return request.getfixturevalue(request.param)


@pytest.fixture(
    scope="function",
    params=[
        ('file_connection_with_raw_schema_id', SOURCE_TYPE_FILE_S3_TABLE),
        ('gsheets_v2_connection_with_raw_schema_id', SOURCE_TYPE_GSHEETS_V2),
    ],
    ids=['file', 'gsheets'],
)
def test_chs3_connections(request) -> tuple[str, CreateDSFrom]:
    conn_id, source_type = request.param
    conn_id = request.getfixturevalue(conn_id)
    return conn_id, source_type


def test_create_connection(client, default_sync_usm_per_test, test_chs3_connection_params):
    conn_params = test_chs3_connection_params
    usm = default_sync_usm_per_test

    # create connection
    resp = client.post(
        '/api/v1/connections',
        data=json.dumps(conn_params),
        content_type='application/json'
    )
    assert resp.status_code == 200
    conn_id = resp.json['id']

    # get connection
    resp = client.get(f'/api/v1/connections/{conn_id}')
    assert resp.status_code == 200
    conn_resp = resp.json
    assert conn_resp['db_type'] == conn_params['type']

    conn = usm.get_by_id(conn_id)
    assert len(conn.data.sources) == 4


def test_source_params(client, test_chs3_connections):
    conn_id, source_type = test_chs3_connections

    sources_resp = client.get(f'/api/v1/connections/{conn_id}/info/sources')
    assert sources_resp.status_code == 200, sources_resp.json
    assert all(set(source['parameters'].keys()) == {'origin_source_id'} for source in sources_resp.json['sources'])


@pytest.mark.parametrize(
    'file_status, expected_status',
    (
        (FileProcessingStatus.ready, 200),
        (FileProcessingStatus.in_progress, 400),
    )
)
def test_add_dataset_source(client, default_sync_usm, api_v1, clickhouse_table, test_chs3_connections, file_status, expected_status):
    us_manager = default_sync_usm
    conn_id, source_type = test_chs3_connections
    conn = us_manager.get_by_id(conn_id, BaseFileS3Connection)
    assert isinstance(conn, BaseFileS3Connection)
    conn.update_data_source(
        id='source_1_id',
        role=DataSourceRole.origin,
        status=file_status,
    )
    us_manager.save(conn)

    ds = Dataset()

    ds.sources['source_1'] = ds.source(
        source_type=source_type,
        connection_id=conn_id,
        parameters=dict(
            origin_source_id='source_1_id',
            status=conn.data.sources[0].status.name,
        )
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == expected_status, ds_resp.json
    ds = ds_resp.dataset
    ds_resp = api_v1.save_dataset(dataset=ds)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors

    conn.update_data_source(
        id='source_1_id',
        role=DataSourceRole.origin,
        status=FileProcessingStatus.ready,
    )
    us_manager.save(conn)
    ds = ds_resp.dataset
    dataset_id = ds.id
    client.delete('/api/v1/datasets/{}'.format(dataset_id))


def test_result(client, api_v1, data_api_v2, test_chs3_connections, s3_native_from_ch_table):
    data_api = data_api_v2
    conn_id, source_type = test_chs3_connections

    sources_resp = client.get(f'/api/v1/connections/{conn_id}/info/sources')
    assert sources_resp.status_code == 200, sources_resp.json

    source_for_ds = {
        k: v for k, v in sources_resp.json['sources'][2].items()
        if k in ('source_type', 'connection_id', 'parameters')
    }

    ds = Dataset()
    ds.sources['source_1'] = ds.source(**source_for_ds)
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
    data_api_v2.get_preview(dataset=ds)

    ds = ds_resp.dataset
    dataset_id = ds.id
    client.delete('/api/v1/datasets/{}'.format(dataset_id))


def test_date32(client, api_v1, data_api_v2, test_chs3_connections, s3_native_from_ch_with_date32_table):
    data_api = data_api_v2
    conn_id, source_type = test_chs3_connections
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        source_type=source_type,
        connection_id=conn_id,
        parameters=dict(origin_source_id='source_4_id'),
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()

    ds.result_schema['date_diff'] = ds.field(formula="[date32_val_2] - [date32_val_1]")
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    ds = api_v1.save_dataset(ds).dataset

    # Just make sure that we can build the query
    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title='date_diff'),
            ds.find_field(title='date32_val_1'),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json

    data_rows = get_data_rows(result_resp)
    assert data_rows == [['-1', '1963-04-02']]
    data_api_v2.get_preview(dataset=ds)

    ds = ds_resp.dataset
    dataset_id = ds.id
    client.delete('/api/v1/datasets/{}'.format(dataset_id))


def test_table_name_spoofing(
        client,
        api_v1,
        data_api_v2,
        default_sync_usm_per_test,
        test_chs3_connections,
        s3_native_from_ch_table,
):
    data_api = data_api_v2
    conn_id, source_type = test_chs3_connections

    usm = default_sync_usm_per_test
    conn: BaseFileS3Connection = usm.get_by_id(conn_id)

    fake_filename = 'hack_me.native'
    orig_filename = 'my_file_2_1.native'
    fake_parameters = {
        'db_name': 'fake db_name',
        'db_version': 'fake db_version',
        'table_name': fake_filename,
        'origin_source_id': 'fake_source_id',
    }

    resp = client.put(
        '/api/v1/connections/{}'.format(conn_id),
        data=json.dumps({
            "sources": [
                {
                    "id": conn.data.sources[2].id,
                    "title": conn.data.sources[2].title,
                    "s3_filename": fake_filename,
                }
            ],
        }),
        content_type='application/json'
    )
    assert resp.status_code == 400, resp.json
    assert resp.json['sources']['0']['s3_filename'] == ['Unknown field.']
    conn = usm.get_by_id(conn_id)
    assert len(conn.data.sources) == 4
    assert conn.data.sources[2].s3_filename == orig_filename

    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        source_type=source_type,
        connection_id=conn_id,
        parameters=fake_parameters,
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()

    ds.result_schema['sum'] = ds.field(formula='SUM([int_value])')
    ds.result_schema['max'] = ds.field(formula='MAX([int_value])')
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST, ds_resp.response_errors

    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        source_type=source_type,
        connection_id=conn_id,
        parameters=dict(
            origin_source_id=conn.data.sources[2].id,
        ),
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()

    ds.result_schema['sum'] = ds.field(formula='SUM([int_value])')
    ds.result_schema['max'] = ds.field(formula='MAX([int_value])')
    ds_resp = api_v1.apply_updates(dataset=ds)
    ds = ds_resp.dataset
    assert len(ds.sources) == 1

    ds = api_v1.save_dataset(ds).dataset

    ds.sources[0].parameters = fake_parameters
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

    preview_resp = data_api_v2.get_preview(dataset=ds, fail_ok=True)
    assert preview_resp.status_code == HTTPStatus.BAD_REQUEST, preview_resp.json
    # ^ because of fake origin_source_id in parameters

    dataset_id = ds.id
    client.delete('/api/v1/datasets/{}'.format(dataset_id))


def test_distinct(client, api_v1, data_api_v2, test_chs3_connections, s3_native_from_ch_table):
    data_api = data_api_v2
    conn_id, source_type = test_chs3_connections
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        source_type=source_type,
        connection_id=conn_id,
        parameters=dict(origin_source_id='source_3_id'),
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()

    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    ds = api_v1.save_dataset(ds).dataset

    distinct_resp = data_api.get_distinct(dataset=ds, field=ds.find_field(title='int_value'))
    assert distinct_resp.status_code == HTTPStatus.OK, distinct_resp.json
    values = [item[0] for item in get_data_rows(distinct_resp)]
    assert len(values) == 10
    assert values == sorted(set(values))

    ds = ds_resp.dataset
    dataset_id = ds.id
    client.delete('/api/v1/datasets/{}'.format(dataset_id))


def test_file_not_configured_add_source(client, api_v1, test_chs3_connections, s3_native_from_ch_table):
    conn_id, source_type = test_chs3_connections
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        source_type=source_type,
        connection_id=conn_id,
        parameters=dict(origin_source_id='source_2_id'),
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()

    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST, ds_resp.response_errors

    ds = ds_resp.dataset
    dataset_id = ds.id
    client.delete('/api/v1/datasets/{}'.format(dataset_id))


def test_file_not_ready_fetch_data(
        client, default_sync_usm, api_v1, data_api_v2,
        test_chs3_connections, s3_native_from_ch_table,
):
    us_manager = default_sync_usm
    conn_id, source_type = test_chs3_connections
    conn: BaseFileS3Connection = us_manager.get_by_id(conn_id)
    data_api = data_api_v2
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        source_type=source_type,
        connection_id=conn_id,
        parameters={
            'origin_source_id': 'source_3_id',
            'status': conn.data.sources[2].status.name,
        },
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()

    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    ds = api_v1.save_dataset(ds).dataset

    # test with status 'in_progress'
    conn = us_manager.get_by_id(conn_id, BaseFileS3Connection)
    assert isinstance(conn, BaseFileS3Connection)
    conn.update_data_source(
        id='source_3_id',
        role=DataSourceRole.origin,
        status=FileProcessingStatus.in_progress,
    )
    us_manager.save(conn)
    distinct_resp = data_api.get_distinct(dataset=ds, field=ds.find_field(title='int_value'), fail_ok=True)
    assert distinct_resp.status_code == HTTPStatus.BAD_REQUEST, distinct_resp.json

    # test with raw schema removed
    conn = us_manager.get_by_id(conn_id, BaseFileS3Connection)
    assert isinstance(conn, BaseFileS3Connection)
    schema_backup = conn.data.sources[2].raw_schema
    conn.update_data_source(
        id='source_3_id',
        role=DataSourceRole.origin,
        status=FileProcessingStatus.ready,
        remove_raw_schema=True,
    )
    us_manager.save(conn)
    distinct_resp = data_api.get_distinct(dataset=ds, field=ds.find_field(title='int_value'), fail_ok=True)
    assert distinct_resp.status_code == HTTPStatus.OK, distinct_resp.json
    # ^ should be OK because we have raw schema in the dataset

    # put everything together again
    conn.update_data_source(
        id='source_3_id',
        role=DataSourceRole.origin,
        status=FileProcessingStatus.ready,
        raw_schema=schema_backup,
    )
    us_manager.save(conn)

    ds = ds_resp.dataset
    dataset_id = ds.id
    client.delete('/api/v1/datasets/{}'.format(dataset_id))


def test_get_data_from_removed_file(
        client,
        api_v1,
        data_api_v2,
        test_chs3_connections,
        s3_native_from_ch_table,
        default_sync_usm_per_test,
):
    data_api = data_api_v2
    usm = default_sync_usm_per_test
    conn_id, source_type = test_chs3_connections
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        source_type=source_type,
        connection_id=conn_id,
        parameters=dict(origin_source_id='source_3_id'),
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()

    ds.result_schema['sum'] = ds.field(formula='SUM([int_value])')
    ds.result_schema['max'] = ds.field(formula='MAX([int_value])')
    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
    ds = ds_resp.dataset
    ds = api_v1.save_dataset(ds).dataset

    preview_resp = data_api.get_preview(dataset=ds)
    assert preview_resp.status_code == 200, preview_resp.json

    # remove source and request preview
    conn: BaseFileS3Connection = usm.get_by_id(conn_id, BaseFileS3Connection)
    update_resp = client.put(
        '/api/v1/connections/{}'.format(conn_id),
        data=json.dumps({
            "sources": [
                {
                    "id": conn.data.sources[0].id,
                    "title": conn.data.sources[0].title,
                },
                {
                    "id": conn.data.sources[1].id,
                    "title": conn.data.sources[1].title,
                },
            ],
        }),
        content_type='application/json',
    )
    assert update_resp.status_code == 200, update_resp.json

    get_ds_resp = client.get(f'/api/v1/datasets/{ds.id}/versions/draft')
    assert get_ds_resp.status_code == 200, get_ds_resp.json

    refresh_resp = api_v1.refresh_dataset_sources(ds, [ds.sources[0].id], fail_ok=True)
    assert refresh_resp.status_code == 400
    assert refresh_resp.json['code'] == 'ERR.DS_API.VALIDATION.ERROR'

    preview_resp = data_api.get_preview(dataset=ds, fail_ok=True)
    assert preview_resp.status_code == 400, preview_resp.json
    assert preview_resp.json['code'] == 'ERR.DS_API.DB.SOURCE_DOES_NOT_EXIST'

    ds = ds_resp.dataset
    dataset_id = ds.id
    client.delete('/api/v1/datasets/{}'.format(dataset_id))


def test_update_dataset_source(client, default_sync_usm, api_v1, clickhouse_table, file_connection_with_raw_schema_id):
    us_manager = default_sync_usm
    conn_id = file_connection_with_raw_schema_id
    conn: BaseFileS3Connection = us_manager.get_by_id(conn_id, BaseFileS3Connection)

    sources_resp = client.get(f'/api/v1/connections/{conn_id}/info/sources')
    source_for_ds = {
        k: v for k, v in sources_resp.json['sources'][0].items()
        if k in ('source_type', 'connection_id', 'parameters')
    }

    ds = Dataset()

    ds.sources['source_1'] = ds.source(**source_for_ds)
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds_resp = api_v1.apply_updates(dataset=ds)
    assert ds_resp.status_code == 200
    ds = ds_resp.dataset

    old_raw_schema = ds.sources[0].raw_schema

    ds_resp = api_v1.save_dataset(dataset=ds)
    assert ds_resp.status_code == 200, ds_resp.response_errors
    ds = ds_resp.dataset

    # --- change connection source: add column ---
    conn.update_data_source(
        id='source_1_id',
        role=DataSourceRole.origin,
        raw_schema=[
            SchemaColumn(title='field1', name='string', user_type=BIType.string),
            SchemaColumn(title='field2', name='date', user_type=BIType.date),
            SchemaColumn(title='field3', name='integer', user_type=BIType.integer),
        ],
    )
    us_manager.save(conn)
    # --- ------------------------------------ ---

    refresh_resp = api_v1.refresh_dataset_sources(ds, [ds.sources['source_1'].id])
    assert refresh_resp.status_code == 200, refresh_resp.json
    ds = refresh_resp.dataset

    new_raw_schema = ds.sources[0].raw_schema

    assert len(new_raw_schema) == len(old_raw_schema) + 1

    dataset_id = ds.id
    client.delete('/api/v1/datasets/{}'.format(dataset_id))
