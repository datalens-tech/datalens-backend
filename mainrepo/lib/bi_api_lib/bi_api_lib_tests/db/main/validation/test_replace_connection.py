from __future__ import annotations

import re
from contextlib import contextmanager
from http import HTTPStatus

from bi_constants.enums import CreateDSFrom

from bi_core.united_storage_client import UStorageClientBase

from bi_connector_mssql.core.constants import SOURCE_TYPE_MSSQL_TABLE
from bi_connector_mysql.core.constants import SOURCE_TYPE_MYSQL_TABLE

from bi_api_lib_tests.utils import (
    make_dataset_for_replacing as make_dataset,
    _make_dataset_for_replacing as get_dataset_response,
    replace_dataset_connection as replace_connection,
)


def test_ch_to_other_sql(api_v1, data_api_v1, clickhouse_db, mysql_db, static_connection_id, mysql_connection_id):
    old_connection_id, new_connection_id = static_connection_id, mysql_connection_id
    old_db, new_db = clickhouse_db, mysql_db

    ds = make_dataset(api_v1, old_connection_id=old_connection_id, old_db=old_db, new_db=new_db, multitable=True)

    ds_resp = replace_connection(
        api_v1, ds=ds, old_connection_id=old_connection_id, new_connection_id=new_connection_id
    )
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    assert ds.sources[0].source_type == SOURCE_TYPE_MYSQL_TABLE
    assert ds.sources[1].source_type == SOURCE_TYPE_MYSQL_TABLE
    data_api_v1.get_preview(dataset=ds)

    api_v1.save_dataset(dataset=ds)


def test_ch_to_nonexistent_table(api_v1, clickhouse_db, mysql_db, static_connection_id, mysql_connection_id):
    old_connection_id, new_connection_id = static_connection_id, mysql_connection_id
    old_db = clickhouse_db

    ds = make_dataset(api_v1, old_connection_id=old_connection_id, old_db=old_db, multitable=True)

    ds = api_v1.save_dataset(dataset=ds).dataset

    result_schema_len = len(ds.result_schema)
    ds_resp = replace_connection(
        api_v1, ds=ds, old_connection_id=old_connection_id,
        new_connection_id=new_connection_id, fail_ok=True
    )
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    assert ds.sources[0].source_type == SOURCE_TYPE_MYSQL_TABLE
    assert ds.sources[1].source_type == SOURCE_TYPE_MYSQL_TABLE
    assert len(ds.result_schema) == result_schema_len  # make sure the fields weren't deleted

    ds_resp = replace_connection(
        api_v1, ds=ds, old_connection_id=new_connection_id,
        new_connection_id=old_connection_id, fail_ok=True
    )
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[
        ds.sources[0].update(parameters=dict(ds.sources[0].parameters, db_name=clickhouse_db.name)),
        ds.sources[1].update(parameters=dict(ds.sources[1].parameters, db_name=clickhouse_db.name)),
    ])
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    assert ds.sources[0].source_type == CreateDSFrom.CH_TABLE
    assert ds.sources[1].source_type == CreateDSFrom.CH_TABLE
    assert len(ds.result_schema) == result_schema_len  # make sure the fields weren't deleted

    api_v1.save_dataset(dataset=ds)


def test_connection_with_only_execute_rights(
    monkeypatch, api_v1, pg_connection_id, mssql_connection_id, postgres_db, mssql_db,
):
    def mock_permissions(client: UStorageClientBase, response: UStorageClientBase.ResponseAdapter):
        json = response.json()
        json['permissions'] = {'execute': True, 'read': False, 'edit': False, 'admin': False}
        return json

    def unmock_permissions(client: UStorageClientBase, response: UStorageClientBase.ResponseAdapter):
        json = response.json()
        return json

    @contextmanager
    def set_only_execute_permissions_for_us_entries():
        monkeypatch.setattr(UStorageClientBase, "_get_us_json_from_response", mock_permissions)
        try:
            yield
        finally:
            monkeypatch.setattr(UStorageClientBase, "_get_us_json_from_response", unmock_permissions)

    with set_only_execute_permissions_for_us_entries():
        ds_resp = get_dataset_response(api_v1, old_connection_id=pg_connection_id, old_db=postgres_db,
                                       new_db=mssql_db,  multitable=True, fail_ok=True,
                                       old_schema='public', new_schema='dbo')  # default schemas for these connections
        assert ds_resp.status_code == HTTPStatus.FORBIDDEN
        assert re.match(r'No permission read for entry \S+', ds_resp.json['message']) is not None

    ds_resp = get_dataset_response(api_v1, old_connection_id=pg_connection_id, old_db=postgres_db,
                                   new_db=mssql_db,  multitable=True, fail_ok=True,
                                   old_schema='public', new_schema='dbo')  # default schemas for these connections
    assert ds_resp.status_code == HTTPStatus.OK

    with set_only_execute_permissions_for_us_entries():
        ds_resp = replace_connection(
            api_v1,
            ds=ds_resp.dataset,
            old_connection_id=pg_connection_id,
            new_connection_id=mssql_connection_id,
            fail_ok=True,
        )

        assert ds_resp.status_code == HTTPStatus.FORBIDDEN
        assert re.match(r'No permission read for entry \S+', ds_resp.json['message']) is not None

    ds_resp = replace_connection(
        api_v1,
        ds=ds_resp.dataset,
        old_connection_id=pg_connection_id,
        new_connection_id=mssql_connection_id,
        fail_ok=True,
    )
    assert ds_resp.status_code == HTTPStatus.OK

    api_v1.save_dataset(dataset=ds_resp.dataset)


def test_with_different_default_schema_names(
        api_v1, data_api_v1,
        postgres_db, mssql_db, pg_connection_id, mssql_connection_id
):
    old_connection_id, new_connection_id = pg_connection_id, mssql_connection_id
    old_db, new_db = postgres_db, mssql_db

    ds = make_dataset(api_v1, old_connection_id=old_connection_id, old_db=old_db, new_db=new_db, multitable=True,
                      old_schema='public', new_schema='dbo')  # default schemas for these connections

    ds_resp = replace_connection(
        api_v1, ds=ds, old_connection_id=old_connection_id, new_connection_id=new_connection_id
    )
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    assert ds.sources[0].source_type == SOURCE_TYPE_MSSQL_TABLE
    assert ds.sources[1].source_type == SOURCE_TYPE_MSSQL_TABLE
    data_api_v1.get_preview(dataset=ds)

    api_v1.save_dataset(dataset=ds)


def test_replace_deleted_connection(client, api_v1, data_api_v1, clickhouse_db, mysql_db,
                                    connection_id, mysql_connection_id):
    old_connection_id, new_connection_id = connection_id, mysql_connection_id
    old_db, new_db = clickhouse_db, mysql_db

    ds = make_dataset(api_v1, old_connection_id=old_connection_id, old_db=old_db, new_db=new_db, multitable=True)

    client.delete(f'/api/v1/connections/{old_connection_id}')

    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=True)
    assert ds_resp.status_code == HTTPStatus.BAD_REQUEST
    ds = ds_resp.dataset
    source_id = ds.sources[0].id
    source_errors = ds.component_errors.get_pack(id=source_id).errors
    assert len(source_errors) == 1
    assert all(re.match(r'Connection for \S+ not found', err.message) for err in source_errors)

    ds_resp = replace_connection(
        api_v1, ds=ds, old_connection_id=old_connection_id, new_connection_id=new_connection_id
    )
    assert ds_resp.status_code == HTTPStatus.OK
    ds = ds_resp.dataset
    assert ds.sources[0].source_type == SOURCE_TYPE_MYSQL_TABLE
    assert ds.sources[1].source_type == SOURCE_TYPE_MYSQL_TABLE
    data_api_v1.get_preview(dataset=ds)

    api_v1.save_dataset(dataset=ds)
