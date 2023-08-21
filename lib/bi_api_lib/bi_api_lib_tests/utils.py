from __future__ import annotations

from http import HTTPStatus
import json
import random
import string
from typing import Callable, List, Optional, Sequence
import uuid

from bi_constants.enums import BIType, CreateDSFrom

from bi_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1, HttpDatasetApiResponse
from bi_api_client.dsmaker.primitives import Dataset

from bi_core_testing.database import C, Db, DbTable, make_table, make_table_with_arrays
from bi_core_testing.dataset import get_created_from

from bi_api_lib.enums import DatasetAction

from bi_connector_mssql.core.constants import SOURCE_TYPE_MSSQL_TABLE
from bi_connector_oracle.core.constants import SOURCE_TYPE_ORACLE_TABLE
from bi_connector_postgresql.core.postgresql.constants import SOURCE_TYPE_PG_TABLE


METRIKA_SAMPLE_COUNTER_ID = '44147844'
APPMETRICA_SAMPLE_COUNTER_ID = '1111'


def make_connection_get_id(connection_params, client, request):
    resp = client.post(
        '/api/v1/connections',
        content_type='application/json',
        data=json.dumps(connection_params),
    )
    assert resp.status_code == 200, resp.json
    conn_id = resp.json['id']

    def teardown():
        client.delete('/api/v1/connections/{}'.format(conn_id))

    request.addfinalizer(teardown)

    return conn_id


def _make_dataset_for_replacing(
        api_v1, old_connection_id, old_db, new_db=None, old_schema=None, new_schema=None,
        multitable: bool = True, fail_ok: bool = False,
) -> HttpDatasetApiResponse:
    table_name_1 = f'test-table-{str(uuid.uuid4())}'
    table_name_2 = f'test-table-{str(uuid.uuid4())}'

    columns = C.full_house()
    # TODO: fix the uuid column behavior.
    columns = [col for col in columns if col.user_type != BIType.uuid]
    old_table_1 = make_table(db=old_db, schema=old_schema, name=table_name_1, columns=columns)
    if new_db:
        # with the same name
        make_table(db=new_db, schema=new_schema, name=table_name_1, columns=columns)

    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=old_connection_id, **data_source_settings_from_table(old_table_1))
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()

    if multitable:
        old_table_2 = make_table(db=old_db, schema=old_schema, name=table_name_2, columns=columns)
        if new_db:
            # with the same name
            make_table(db=new_db, schema=new_schema, name=table_name_2, columns=columns)
        ds.sources['source_2'] = ds.source(
            connection_id=old_connection_id, **data_source_settings_from_table(old_table_2))
        ds.source_avatars['avatar_2'] = ds.sources['source_2'].avatar()
        ds.avatar_relations['relation_1'] = ds.source_avatars['avatar_1'].join(
            ds.source_avatars['avatar_2']
        ).on(
            ds.col('int_value') == ds.col('int_value'),
        )

    ds_resp = api_v1.apply_updates(dataset=ds, fail_ok=fail_ok)
    if not fail_ok:
        assert ds_resp.status_code == HTTPStatus.OK

    return ds_resp


def make_dataset_for_replacing(
        api_v1, old_connection_id, old_db, new_db=None, old_schema=None, new_schema=None,
        multitable: bool = True, fail_ok: bool = False,
) -> Dataset:
    ds_resp = _make_dataset_for_replacing(api_v1, old_connection_id=old_connection_id, old_db=old_db, new_db=new_db,
                                          old_schema=old_schema, new_schema=new_schema, multitable=multitable,
                                          fail_ok=fail_ok)
    return ds_resp.dataset


def replace_dataset_connection(
        api_v1: SyncHttpDatasetApiV1, ds: Dataset,
        old_connection_id: str, new_connection_id: str,
        fail_ok: bool = False
) -> HttpDatasetApiResponse:
    ds_resp = api_v1.apply_updates(dataset=ds, updates=[
        {
            'action': DatasetAction.replace_connection.value,
            'connection': {
                'id': old_connection_id,
                'new_id': new_connection_id
            },
        },
    ], fail_ok=fail_ok)
    return ds_resp


def get_result_schema(client, dataset_id: str) -> List[dict]:
    return get_dataset_data(client, dataset_id)['dataset']['result_schema']


def get_dataset_data(client, dataset_id: str) -> dict:
    response = client.get(
        '/api/v1/datasets/{}/versions/draft'.format(dataset_id)
    )
    assert response.status_code == 200
    return response.json


def validate_schema(
        client, dataset_id: str,
        update_batch: Optional[List[dict]] = None,
        dataset_data: Optional[dict] = None
):
    data = dataset_data.copy() if dataset_data is not None else {}
    data['updates'] = update_batch or []
    return client.post(
        '/api/v1/datasets/{}/versions/draft/validators/schema'.format(dataset_id),
        data=json.dumps(data),
        content_type='application/json'
    )


def get_field_by_title(result_schema: List[dict], title) -> dict:
    return next(filter(lambda f: f['title'] == title, result_schema))


def data_source_settings_from_table(table: DbTable):
    source_type = get_created_from(db=table.db)
    data = {  # this still requires connection_id to be defined
        'source_type': source_type,
        'parameters': {
            'table_name': table.name,
            'db_name': table.db.name if source_type == CreateDSFrom.CH_TABLE else None,
        },
    }

    if source_type in (SOURCE_TYPE_PG_TABLE, SOURCE_TYPE_MSSQL_TABLE, SOURCE_TYPE_ORACLE_TABLE):
        data['parameters']['schema_name'] = table.schema

    return data


def recycle_validation_response(body: dict) -> dict:
    """Prepare dataset validation input data from previous validation response"""
    return {key: value for key, value in body.items() if key in (
        'dataset',
    )}


def get_random_str(length=10):
    return ''.join(
        random.choice(string.ascii_letters)
        for _ in range(length))


def make_dataset_with_tree(
        api_v1: SyncHttpDatasetApiV1,
        db: Db,
        connection_id: str,
        array_data: Optional[List[List[str]]] = None,
        array_callable: Callable[[int], List[str]] = lambda rn: [str(i) for i in range(rn)],
        rows: int = 10,
        additional_columns: Sequence[C] = ()
) -> Dataset:

    if array_data is not None:
        array_callable = lambda rn: array_data[rn]  # noqa
        rows = len(array_data)

    columns = [
        C('const_value', BIType.integer, vg=lambda rn, **kwargs: 0),
        C('int_value', BIType.integer, vg=lambda rn, **kwargs: rn),
        C('array_str_value', BIType.tree_str, vg=lambda rn, **kwargs: array_callable(rn)),
        *additional_columns,
    ]
    db_table = make_table_with_arrays(db, columns=columns, rows=rows)
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=connection_id, **data_source_settings_from_table(db_table))
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds.result_schema['tree_str_value'] = ds.field(formula='TREE([array_str_value])')
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds = api_v1.save_dataset(ds).dataset
    return ds
