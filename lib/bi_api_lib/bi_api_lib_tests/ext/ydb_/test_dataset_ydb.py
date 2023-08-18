from __future__ import annotations

import json
import pytest

from bi_testing.test_data.sql_queries import YDB_QUERY_FULL
from bi_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset

from bi_api_lib_tests.utils import get_result_schema


def req_test_connection(client, conn_id, params=None):
    return client.post(
        f'/api/v1/connections/test_connection/{conn_id}',
        content_type='application/json',
        data=json.dumps(params or {}),
    )


def req_get_sources(client, conn_id):
    return client.get(f'/api/v1/connections/{conn_id}/info/sources')


# Make sure the table exists for all tests in the context
class WithYDBLocalTableCreated:
    @pytest.fixture(scope='session', autouse=True)
    def _ydb_local_table(self, ydb_local_table):
        pass


class TestYDB(WithYDBLocalTableCreated):
    def test_test_connection(self, client, ydb_local_connection_id):
        resp = req_test_connection(client, ydb_local_connection_id)
        assert resp.status_code == 200, resp.json

    def test_ydb_connection_sources(self, client, ydb_local_connection_id):
        """
        Ensure the saved default_... parameters are returned in the source.
        """
        resp = req_get_sources(client=client, conn_id=ydb_local_connection_id)
        resp_data = resp.json
        assert resp.status_code == 200, resp_data
        assert len(resp_data['freeform_sources']) == 2, resp_data

        src_by_st = {item['source_type']: item for item in resp_data['freeform_sources']}
        tbl_src = src_by_st['YDB_TABLE']
        assert tbl_src

        assert resp_data['sources']
        some_source = resp_data['sources'][0]
        assert some_source['source_type'] == 'YDB_TABLE', some_source

    def test_dataset_preview_ydb_table(self, client, data_api_v1, ydb_local_table_dataset):
        dataset_id = ydb_local_table_dataset.id
        resp = data_api_v1.get_response_for_dataset_preview(dataset_id=dataset_id)
        resp_data = resp.json
        assert resp.status_code == 200, resp_data
        assert resp_data['result']['data']['Data']

    def test_dataset_result_ydb_table(self, client, api_v1, data_api_v1, ydb_local_table_dataset):
        ds = ydb_local_table_dataset
        ds = add_formulas_to_dataset(api_v1=api_v1, dataset=ds, formulas=dict(
            some_int32_s='STR([some_int32])',
        ))

        dataset_id = ds.id
        result_schema = get_result_schema(client, dataset_id)
        resp = data_api_v1.get_response_for_dataset_result(
            dataset_id=dataset_id,
            raw_body=dict(
                columns=[
                    col['guid']
                    for col in result_schema
                    if col['data_type'] != 'unsupported'  # e.g. Decimal
                    and col['title'] != 'some_int32'  # groupby-formula test.
                ],
                limit=300,
            ),
        )
        resp_data = resp.json
        assert resp.status_code == 200, resp_data
        schema = resp_data['result']['data']['Type'][-1][-1]
        data = resp_data['result']['data']['Data']

        str_idx = next(idx for idx, (name, _) in enumerate(schema) if name == 'some_string')
        str_vals = set(row[str_idx] for row in data)
        assert 'ff0aff' in str_vals, str_vals  # shouldn't have repr'd `b'ff0aff'`

        ts_idx = next(idx for idx, (name, _) in enumerate(schema) if name == 'some_timestamp')
        ts_vals = [row[ts_idx] for row in data]
        assert any(ts_vals)

    def test_dataset_uint_substraction_result_ydb_table(self, client, api_v1, data_api_v1, ydb_local_table_dataset):
        ds = ydb_local_table_dataset
        ds = add_formulas_to_dataset(api_v1=api_v1, dataset=ds, formulas=dict(
            uint1='[some_uint64] + 1',
            uint2='[some_uint64] - 1',
            uint_substraction='[uint2] - [uint1]',
            float_substraction='[some_float] - 0.5'
        ))

        dataset_id = ds.id
        result_schema = get_result_schema(client, dataset_id)
        cols_to_check = ['uint_substraction', 'uint1', 'uint2', 'float_substraction']
        resp = data_api_v1.get_response_for_dataset_result(
            dataset_id=dataset_id,
            raw_body=dict(
                columns=[
                    col['guid']
                    for col in result_schema
                    if col['data_type'] != 'unsupported'  # e.g. Decimal
                    and col['title'] in cols_to_check
                ],
                limit=300,
            ),
        )
        resp_data = resp.json
        assert resp.status_code == 200, resp_data
        schema = resp_data['result']['data']['Type'][-1][-1]
        data = resp_data['result']['data']['Data']

        sub_uint_idx = next(idx for idx, (name, _) in enumerate(schema) if name == 'uint_substraction')
        sub_uint_vals = [row[sub_uint_idx] for row in data]
        assert any(int(el) < 0 for el in sub_uint_vals if el is not None)

        sub_float_idx = next(idx for idx, (name, _) in enumerate(schema) if name == 'float_substraction')
        sub_float_vals = [row[sub_float_idx] for row in data]
        assert any(float(el) != int(float(el)) for el in sub_float_vals if el is not None)

    def test_dataset_result_filters_ydb_table(self, client, data_api_v1, ydb_local_table_dataset):
        dataset_id = ydb_local_table_dataset.id
        result_schema = get_result_schema(client, dataset_id)
        resp = data_api_v1.get_response_for_dataset_result(
            dataset_id=dataset_id,
            raw_body=dict(
                columns=[
                    col['guid']
                    for col in result_schema
                    if col['data_type'] != 'unsupported'
                ],
                where=[
                    # Test that datetime value serialization works at all.
                    dict(
                        column='some_datetime',
                        operation='BETWEEN',
                        values=['2011-06-07T18:19:20Z', '2031-06-07T18:19:20Z'],
                    ),
                ],
                limit=3,
            ),
        )
        resp_data = resp.json
        assert resp.status_code == 200, resp_data

    @pytest.mark.asyncio
    async def test_ydb_dashsql_result(self, async_api_local_env_low_level_client, ydb_local_connection_id):
        data_api_aio = async_api_local_env_low_level_client
        conn_id = ydb_local_connection_id

        resp = await data_api_aio.request(
            "post", f"/api/v1/connections/{conn_id}/dashsql",
            json={"sql_query": YDB_QUERY_FULL})
        resp_data = await resp.json()
        assert resp.status == 200, resp_data
        assert resp_data[0]["event"] == "metadata", resp_data
        assert resp_data[0]["data"]["names"][:12] == [
            "id", "some_str", "some_utf8",
            "some_int", "some_uint8", "some_int64", "some_uint64",
            "some_double", "some_bool",
            "some_date", "some_datetime", "some_timestamp",
        ]
        assert resp_data[0]["data"]["driver_types"][:12] == [
            "int64?", "string", "utf8?",
            "int32", "uint8?", "int64", "uint64",
            "double", "bool", "date",
            "datetime", "timestamp",
        ]
        assert resp_data[0]["data"]["db_types"][:12] == [
            "integer", "text", "text",
            "integer", "integer", "integer", "integer",
            "float", "boolean", "date",
            "datetime", "datetime",
        ]
        assert resp_data[0]["data"]["bi_types"][:12] == [
            "integer", "string", "string",
            "integer", "integer", "integer", "integer",
            "float", "boolean", "date",
            "genericdatetime", "genericdatetime",
        ]

    @pytest.mark.asyncio
    async def test_ydb_dashsql_user_error(self, async_api_local_env_low_level_client, ydb_local_connection_id):
        data_api_aio = async_api_local_env_low_level_client
        conn_id = ydb_local_connection_id

        resp = await data_api_aio.request(
            "post", f"/api/v1/connections/{conn_id}/dashsql",
            json={"sql_query": "select 1/"})
        resp_data = await resp.json()
        assert resp.status == 400, resp_data
        assert resp_data['code'] == 'ERR.DS_API.DB', resp_data
        assert resp_data.get('details', {}).get('db_message'), resp_data


class TestYDBNonexistentTable(WithYDBLocalTableCreated):
    def test_ydb_nonexistent_preview(self, data_api_v1, ydb_local_table_dataset):
        ds = ydb_local_table_dataset
        ds.sources['source_1'].parameters['table_name'] = 'some_dir/test_table_definitely_nonexistent'
        resp = data_api_v1.get_preview(dataset=ds, fail_ok=True)
        resp_data = resp.json
        assert resp.status_code == 400, resp
        assert resp_data['code'] == 'ERR.DS_API.DB'  # Can't do much better yet.
        assert 'Cannot find table' in resp_data.get('details', {}).get('db_message', '')


class TestYDBAuthenticationFailure:
    def test_test_connection(self, client, ydb_ext_connection_id_failed_authentication):
        resp = req_test_connection(client, ydb_ext_connection_id_failed_authentication)
        assert resp.status_code == 400, resp.json

    def test_ydb_connection_sources(self, client, ydb_ext_connection_id_failed_authentication):
        resp = req_get_sources(client, ydb_ext_connection_id_failed_authentication)
        assert resp.status_code == 400, resp.json


class TestYDBInvalidDB(WithYDBLocalTableCreated):
    def test_ydb_connection_sources(self, client, ydb_local_connection_id_invalid_db):
        resp = req_get_sources(client, ydb_local_connection_id_invalid_db)
        resp_data = resp.json
        assert resp.status_code == 400, resp_data
        assert resp_data['code'] == 'ERR.DS_API.DB'  # Can't do much better yet.
        assert 'Unknown database' in resp_data.get('details', {}).get('db_message', ''), resp_data


class TestYDBAuthorizationFailure:
    def test_ydb_connection_sources(self, client, ydb_ext_connection_id_failed_authorization):
        resp = req_get_sources(client, ydb_ext_connection_id_failed_authorization)
        resp_data = resp.json
        assert resp.status_code == 400, resp_data
        assert resp_data['code'] == 'ERR.DS_API.DB'  # Can't do much better yet.
        assert 'Access denied' in resp_data.get('details', {}).get('db_message', ''), resp_data
