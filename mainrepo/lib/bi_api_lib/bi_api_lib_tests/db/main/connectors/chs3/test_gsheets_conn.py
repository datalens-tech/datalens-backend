from __future__ import annotations

import datetime
import uuid
import json
from http import HTTPStatus

from bi_constants.enums import (
    WhereClauseOperation,
    ComponentType,
    DataSourceRole,
    FileProcessingStatus,
    ComponentErrorLevel,
)

from bi_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from bi_connector_bundle_chs3.chs3_gsheets.core.constants import SOURCE_TYPE_GSHEETS_V2
from bi_connector_bundle_chs3.chs3_gsheets.core.us_connection import GSheetsFileS3Connection
from bi_core.reporting.notifications import NotificationType
from bi_core import exc

from bi_api_client.dsmaker.primitives import Dataset


def test_authorized(
        client,
        default_sync_usm_per_test,
        gsheets_v2_connection_id,
):
    conn_id = gsheets_v2_connection_id
    usm = default_sync_usm_per_test

    # no token == not authorized
    conn: GSheetsFileS3Connection = usm.get_by_id(conn_id)
    assert conn.authorized is False
    conn_resp = client.get(f'/api/v1/connections/{conn_id}')
    assert conn_resp.status_code == 200
    assert conn_resp.json['authorized'] is False
    assert conn_resp.json.get('refresh_token') is None

    default_update_data = {
        "refresh_enabled": True,
        "sources": [
            {
                "id": conn.data.sources[0].id,
                "title": conn.data.sources[0].title,
            },
            {
                "id": conn.data.sources[1].id,
                "title": conn.data.sources[1].title,
            },
            {
                "id": conn.data.sources[2].id,
                "title": conn.data.sources[2].title,
            },
        ],
    }

    # add token
    resp = client.put(
        '/api/v1/connections/{}'.format(conn_id),
        data=json.dumps({
            **default_update_data,
            "refresh_token": "some_token",
        }),
        content_type='application/json'
    )
    assert resp.status_code == 200, resp.json

    conn: GSheetsFileS3Connection = usm.get_by_id(conn_id)
    assert conn.authorized is True
    conn_resp = client.get(f'/api/v1/connections/{conn_id}')
    assert conn_resp.status_code == 200
    assert conn_resp.json['authorized'] is True
    assert conn_resp.json.get('refresh_token') is None

    # remove token
    resp = client.put(
        '/api/v1/connections/{}'.format(conn_id),
        data=json.dumps({
            **default_update_data,
            "refresh_token": None,
        }),
        content_type='application/json'
    )
    assert resp.status_code == 200, resp.json

    conn: GSheetsFileS3Connection = usm.get_by_id(conn_id)
    assert conn.authorized is False
    conn_resp = client.get(f'/api/v1/connections/{conn_id}')
    assert conn_resp.status_code == 200
    assert conn_resp.json['authorized'] is False
    assert conn_resp.json.get('refresh_token') is None


# TODO? mb make non-editable fields (spreadsheet_id, sheet_id, first_line_is_header) actually non-editable
def test_update_gsheets_conn(
        client,
        default_sync_usm_per_test,
        gsheets_v2_connection_with_raw_schema_id,
):
    conn_id = gsheets_v2_connection_with_raw_schema_id
    usm = default_sync_usm_per_test
    conn = usm.get_by_id(conn_id)
    assert len(conn.data.sources) == 4
    assert conn.data.sources[0].raw_schema

    resp = client.put(
        '/api/v1/connections/{}'.format(conn_id),
        data=json.dumps({
            "refresh_enabled": True,
            "sources": [
                {
                    "id": conn.data.sources[0].id,
                    "title": 'renamed source',
                },
                {
                    "id": conn.data.sources[1].id,
                    "title": conn.data.sources[1].title,
                },
                # drop third source
            ],
        }),
        content_type='application/json'
    )
    assert resp.status_code == 200, resp.json
    conn = usm.get_by_id(conn_id)
    assert len(conn.data.sources) == 2
    assert conn.data.sources[0].title == 'renamed source'
    assert conn.data.sources[0].raw_schema

    resp = client.put(
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
                {   # add new source
                    "id": str(uuid.uuid4()),
                    "file_id": str(uuid.uuid4()),
                    "title": "My File 2 - Sheet 2",
                },
            ],
        }),
        content_type='application/json'
    )
    assert resp.status_code == 200, resp.json
    conn = usm.get_by_id(conn_id)
    assert len(conn.data.sources) == 3

    # test replace source
    old_sources = conn.data.sources
    replaced_source = conn.data.sources[0]
    new_source = {
        "id": str(uuid.uuid4()),
        "file_id": str(uuid.uuid4()),
        "title": "My File 3 - Sheet 1",
    }
    resp = client.put(
        '/api/v1/connections/{}'.format(conn_id),
        data=json.dumps({
            "sources": [
                {
                    "id": conn.data.sources[1].id,
                    "title": conn.data.sources[1].title,
                },
                {
                    "id": conn.data.sources[2].id,
                    "title": conn.data.sources[2].title,
                },
                new_source,
            ],
            "replace_sources": [
                {
                    "old_source_id": replaced_source.id,
                    "new_source_id": new_source['id'],
                },
            ]
        }),
        content_type='application/json'
    )
    assert resp.status_code == 200, resp.json
    conn: BaseFileS3Connection = usm.get_by_id(conn_id)

    old_source_ids = set(src.id for src in old_sources)
    new_source_ids = set(src.id for src in conn.data.sources)
    assert old_source_ids == new_source_ids
    new_replaced_source = conn.get_file_source_by_id(replaced_source.id)
    assert new_replaced_source.file_id != replaced_source.file_id


def test_consistency_checks(
        client,
        default_sync_usm_per_test,
        gsheets_v2_connection_with_raw_schema_id
):
    conn_id = gsheets_v2_connection_with_raw_schema_id
    usm = default_sync_usm_per_test
    conn = usm.get_by_id(conn_id)
    assert len(conn.data.sources) == 4
    assert conn.data.sources[0].raw_schema

    replaced_source_id = str(uuid.uuid4())  # replacing a non-existent source
    new_source = {  # no file_id
        "id": str(uuid.uuid4()),
        "title": "My File 2",
    }
    resp = client.put(
        '/api/v1/connections/{}'.format(conn_id),
        data=json.dumps({
            "sources": [
                {
                    "id": conn.data.sources[1].id,
                    "title": conn.data.sources[1].title,
                },
                {
                    "id": conn.data.sources[2].id,
                    "title": conn.data.sources[2].title,
                },
                new_source,
            ],
            "replace_sources": [
                {
                    "old_source_id": replaced_source_id,
                    "new_source_id": new_source['id'],
                },
            ]
        }),
        content_type='application/json'
    )
    assert resp.status_code == 400, resp.json
    details: dict[str, list[str]] = resp.json['details']
    assert details == {
        'not_configured_not_saved': [new_source['id']],
        'replaced_not_saved': [replaced_source_id],
    }

    # test not unique titles
    new_source = {
        "id": str(uuid.uuid4()),
        "file_id": str(uuid.uuid4()),
        "title": conn.data.sources[1].title,
    }
    resp = client.put(
        '/api/v1/connections/{}'.format(conn_id),
        data=json.dumps({
            "sources": [
                {
                    "id": conn.data.sources[1].id,
                    "title": conn.data.sources[1].title,
                },
                new_source,
            ],
        }),
        content_type='application/json'
    )
    assert resp.status_code == 400, resp.json
    err_message: str = resp.json['message']
    assert err_message == exc.DataSourceTitleConflict().message


def test_update_data(
        client,
        default_sync_usm_per_test,
        gsheets_v2_connection_with_raw_schema_id,
        api_v1,
        data_api_v2,
        s3_native_from_ch_table,
):
    conn_id = gsheets_v2_connection_with_raw_schema_id
    usm = default_sync_usm_per_test
    data_api = data_api_v2

    conn: GSheetsFileS3Connection = usm.get_by_id(conn_id)
    dt_now = datetime.datetime.now(datetime.timezone.utc)
    data_updated_at_orig = dt_now
    for src in conn.data.sources:
        src.data_updated_at = dt_now
    usm.save(conn)

    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        source_type=SOURCE_TYPE_GSHEETS_V2,
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

    def make_request_result():
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
        return result_resp

    resp = make_request_result()
    assert all(
        notification['locator'] != NotificationType.stale_data.value
        for notification in resp.json.get('notifications', [])
    ), resp.json.get('notifications', 'No notifications')
    conn: GSheetsFileS3Connection = usm.get_by_id(conn_id)
    assert conn.data.sources[0].data_updated_at == data_updated_at_orig

    data_updated_at = (
        conn.data.oldest_data_update_time()
        -
        datetime.timedelta(minutes=31)
    )
    for src in conn.data.sources:
        src.data_updated_at = data_updated_at
    usm.save(conn)
    resp = make_request_result()
    assert any(
        notification['locator'] == NotificationType.stale_data.value
        for notification in resp.json.get('notifications', [])
    ), resp.json.get('notifications', 'No notifications')
    conn: GSheetsFileS3Connection = usm.get_by_id(conn_id)
    assert conn.data.sources[0].data_updated_at != data_updated_at

    ds = ds_resp.dataset
    dataset_id = ds.id
    client.delete('/api/v1/datasets/{}'.format(dataset_id))


def test_force_update_gsheets_conn_with_file_id(
        client,
        default_sync_usm_per_test,
        gsheets_v2_connection_with_raw_schema_id
):
    """
    This test is pretty much useless unless we create corresponding records in redis for all test sources
    to see file-uploader-worker process them
    Nevertheless, this test allows one to follow all the steps with the debugger and make sure the update is triggered)
    """

    conn_id = gsheets_v2_connection_with_raw_schema_id
    usm = default_sync_usm_per_test
    conn = usm.get_by_id(conn_id)
    assert len(conn.data.sources) == 4
    assert conn.data.sources[0].raw_schema

    resp = client.put(
        '/api/v1/connections/{}'.format(conn_id),
        data=json.dumps({
            "refresh_enabled": True,
            "sources": [
                {
                    "file_id": 'new_file_id',  # force source update by passing file_id
                    "id": conn.data.sources[0].id,
                    "title": conn.data.sources[0].title,
                },
                {
                    "id": conn.data.sources[1].id,
                    "title": conn.data.sources[1].title,
                },
                {
                    "id": conn.data.sources[2].id,
                    "title": conn.data.sources[2].title,
                },
            ],
        }),
        content_type='application/json'
    )
    assert resp.status_code == 200, resp.json
    conn = usm.get_by_id(conn_id)
    assert len(conn.data.sources) == 3
    assert conn.data.sources[0].raw_schema
    assert conn.data.sources[0].file_id == 'new_file_id'


def test_component_errors(
        client,
        default_sync_usm_per_test,
        gsheets_v2_connection_with_raw_schema_id,
        api_v1,
        data_api_v2,
        s3_native_from_ch_table,
):
    conn_id = gsheets_v2_connection_with_raw_schema_id
    usm = default_sync_usm_per_test
    data_api = data_api_v2

    conn: GSheetsFileS3Connection = usm.get_by_id(conn_id)

    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        source_type=SOURCE_TYPE_GSHEETS_V2,
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

    conn.data.component_errors.add_error(
        id=conn.data.sources[2].id,
        type=ComponentType.data_source,
        message='Custom error message',
        code=['FILE', 'CUSTOM_FILE_ERROR'],
        details={'error': 'details'},
    )
    conn.update_data_source(
        conn.data.sources[2].id,
        role=DataSourceRole.origin,
        s3_filename=None,
        status=FileProcessingStatus.failed,
        preview_id=None,
        data_updated_at=datetime.datetime.now(datetime.timezone.utc),
    )
    usm.save(conn)

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

    assert result_resp.status_code == 400
    assert result_resp.json['details'] == {'error': 'details'}
    assert result_resp.json['message'] == 'Custom error message'
    assert result_resp.json['code'] == 'ERR.DS_API.SOURCE.FILE.CUSTOM_FILE_ERROR'

    resp = client.get(f'/api/v1/connections/{conn_id}')
    assert resp.status_code == 200, resp.json
    assert resp.json['component_errors'], resp.json
    errors = resp.json['component_errors']['items'][0]['errors']
    assert len(errors) == 1, errors
    assert errors[0]['code'] == 'ERR.DS_API.SOURCE.FILE.CUSTOM_FILE_ERROR'

    ds = ds_resp.dataset
    dataset_id = ds.id
    client.delete('/api/v1/datasets/{}'.format(dataset_id))


def test_component_error_warning(
        client,
        default_sync_usm_per_test,
        gsheets_v2_connection_with_raw_schema_id,
        api_v1,
        data_api_v2,
        s3_native_from_ch_table,
):
    conn_id = gsheets_v2_connection_with_raw_schema_id
    usm = default_sync_usm_per_test
    data_api = data_api_v2

    conn: GSheetsFileS3Connection = usm.get_by_id(conn_id)

    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        source_type=SOURCE_TYPE_GSHEETS_V2,
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

    conn.data.component_errors.add_error(
        id=conn.data.sources[2].id,
        type=ComponentType.data_source,
        message='Custom error message',
        code=['FILE', 'CUSTOM_FILE_ERROR'],
        details={'error': 'details', 'request-id': '637'},
        level=ComponentErrorLevel.warning,
    )
    long_long_ago = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=31)
    conn.update_data_source(
        conn.data.sources[2].id,
        role=DataSourceRole.origin,
        data_updated_at=long_long_ago,
    )
    usm.save(conn)

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

    assert result_resp.status_code == 200, result_resp.json
    assert len(result_resp.json['notifications']) == 2
    assert 'Reason: FILE.CUSTOM_FILE_ERROR, Request-ID: 637' in result_resp.json['notifications'][0]['message']
    conn: GSheetsFileS3Connection = usm.get_by_id(conn_id)
    assert conn.data.sources[2].data_updated_at != long_long_ago

    ds = ds_resp.dataset
    dataset_id = ds.id
    client.delete('/api/v1/datasets/{}'.format(dataset_id))
