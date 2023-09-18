from __future__ import annotations

import uuid
import json

from dl_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from dl_core import exc


def test_update_file_conn(
        client,
        default_sync_usm_per_test,
        file_connection_with_raw_schema_id
):
    conn_id = file_connection_with_raw_schema_id
    usm = default_sync_usm_per_test
    conn = usm.get_by_id(conn_id)
    assert len(conn.data.sources) == 4
    assert conn.data.sources[0].raw_schema

    resp = client.put(
        '/api/v1/connections/{}'.format(conn_id),
        data=json.dumps({
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
    assert resp.status_code == 200
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
                    "column_types": [
                        {"name": "field1", "user_type": "string"},
                        {"name": "field2", "user_type": "integer"},
                    ]
                },
            ],
        }),
        content_type='application/json'
    )
    assert resp.status_code == 200
    conn = usm.get_by_id(conn_id)
    assert len(conn.data.sources) == 3

    # test replace source
    old_sources = conn.data.sources
    replaced_source = conn.data.sources[0]
    new_source = {
        "id": str(uuid.uuid4()),
        "file_id": str(uuid.uuid4()),
        "title": "My File 3 - Sheet 1",
        "column_types": [
            {"name": "field1", "user_type": "string"},
            {"name": "field2", "user_type": "integer"},
        ]
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
        file_connection_with_raw_schema_id
):
    conn_id = file_connection_with_raw_schema_id
    usm = default_sync_usm_per_test
    conn = usm.get_by_id(conn_id)
    assert len(conn.data.sources) == 4
    assert conn.data.sources[0].raw_schema

    replaced_source_id = str(uuid.uuid4())  # replacing a non-existent source
    new_source = {  # no file_id
        "id": str(uuid.uuid4()),
        "title": "My File 2",
        "column_types": [
            {"name": "field1", "user_type": "string"},
            {"name": "field2", "user_type": "integer"},
        ]
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
        "column_types": [
            {"name": "field1", "user_type": "string"},
            {"name": "field2", "user_type": "integer"},
        ]
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
