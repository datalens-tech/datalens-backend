import json
import os
import pytest
import ydb
from collections import OrderedDict

from bi_api_client.dsmaker.primitives import Dataset
from bi_testing.containers import get_test_container_hostport

from bi_api_lib_tests.utils import get_random_str

from bi_connector_yql.core.ydb.constants import SOURCE_TYPE_YDB_TABLE


YDB_CONNECTION_PARAMS_BASE = {
    'name': 'ydb_test_{}'.format(get_random_str()),
    'type': 'ydb',
    'raw_sql_level': 'dashsql',  # NOTE: allowing by default
}


def _get_ydb_local_connection_params(db_name='/local'):
    return dict(
        YDB_CONNECTION_PARAMS_BASE,
        host=get_test_container_hostport('db-ydb', fallback_port=50481).host,
        port=get_test_container_hostport('db-ydb', fallback_port=50481).port,
        db_name=db_name,
    )


def _get_ydb_ext_connection_params(token, db_name='/ru-prestable/yql/test/datalens-ydb-integration-test'):
    return dict(
        YDB_CONNECTION_PARAMS_BASE,
        host='ydb-ru-prestable.yandex.net',
        port=2135,
        token=token,
        db_name=db_name,
    )


def _make_ydb_connection_id_fixture(client, connection_params, request):
    resp = client.post(
        '/api/v1/connections',
        data=json.dumps(connection_params),
        content_type='application/json',
    )
    assert resp.status_code == 200, resp.json
    conn_id = resp.json['id']

    def teardown():
        client.delete('/api/v1/connections/{}'.format(conn_id))

    request.addfinalizer(teardown)

    return conn_id


TABLE_DESCRIPTION = OrderedDict((
    ('id', ydb.PrimitiveType.Int64),

    ('some_int32', ydb.PrimitiveType.Int32),
    ('some_int64', ydb.PrimitiveType.Int64),
    ('some_uint8', ydb.PrimitiveType.Uint8),
    ('some_uint32', ydb.PrimitiveType.Uint32),
    ('some_uint64', ydb.PrimitiveType.Uint64),
    ('some_bool', ydb.PrimitiveType.Bool),

    ('some_float', ydb.PrimitiveType.Float),
    ('some_double', ydb.PrimitiveType.Double),

    ('some_string', ydb.PrimitiveType.String),
    ('some_utf8', ydb.PrimitiveType.Utf8),

    ('some_date', ydb.PrimitiveType.Date),
    ('some_datetime', ydb.PrimitiveType.Datetime),
    ('some_timestamp', ydb.PrimitiveType.Timestamp),
    ('some_interval', ydb.PrimitiveType.Interval),
))

SAMPLE_DATA = (
    {
        'columns': ('id', 'some_int32', 'some_int64', 'some_uint8', 'some_uint32', 'some_uint64', 'some_bool'),
        'values': (
            (2, 1073741824, 4611686018427387904, 254, 4294967293, 18446744073709551612, True),
            (102, -1, -2, 3, 4, 5, False),
        ),
    },
    {
        'columns': ('id', 'some_float', 'some_double'),
        'values': (
            (3, 79079710.351049881, 1579079710.351049881),
            (103, 7.8, 9.10),
        ),
    },
    {
        'columns': ('id', 'some_string', 'some_utf8'),
        'values': (
            (5, 'ff0aff',  '… «C≝⋯≅M»!'),
        ),
    },
    {
        'columns': ('id', 'some_date', 'some_datetime', 'some_timestamp'),
        'values': (
            (7, '2021-06-07', '2021-06-07T18:19:20Z', '2021-06-07T18:19:20.029181Z'),
            (107, '1970-12-31', '1970-12-31T23:58:57Z', '1970-12-31T23:58:57.565554Z')
        )
    },
)

VALUE_TRANSFORMERS = {
    ydb.PrimitiveType.Bool: lambda x: 'true' if x else 'false',
    ydb.PrimitiveType.Float: lambda x: f'CAST({x} AS Float)',
    ydb.PrimitiveType.String: lambda x: f'"{x}"',
    ydb.PrimitiveType.Utf8: lambda x: f'"{x}"',
    ydb.PrimitiveType.Date: lambda x: f'Date("{x}")',
    ydb.PrimitiveType.Datetime: lambda x: f'Datetime("{x}")',
    ydb.PrimitiveType.Timestamp: lambda x: f'Timestamp("{x}")',
}
DEFAULT_VALUE_TRANSFORMER = str


def _replace_query(table_name, sample_data_item):
    replace_query_tmpl = 'REPLACE INTO `{table_name}` ({columns}) VALUES {values};'

    values_strs = [
        '(' + ', '.join([
            VALUE_TRANSFORMERS.get(TABLE_DESCRIPTION[column_name], DEFAULT_VALUE_TRANSFORMER)(value)
            for column_name, value in zip(sample_data_item['columns'], value_row)
        ]) + ')'
        for value_row in sample_data_item['values']
    ]

    return replace_query_tmpl.format(
        table_name=table_name,
        columns=', '.join(sample_data_item['columns']),
        values=', '.join(values_strs),
    )


YDB_LOCAL_TABLE_NAME = 'test_table_h'


@pytest.fixture(scope='function')
def ydb_local_connection_id(app, client, request):
    return _make_ydb_connection_id_fixture(
        client,
        _get_ydb_local_connection_params(),
        request,
    )


@pytest.fixture(scope='function')
def ydb_local_connection_id_invalid_db(app, client, request):
    return _make_ydb_connection_id_fixture(
        client,
        _get_ydb_local_connection_params(db_name='/some_db/some_dir'),
        request,
    )


@pytest.fixture(scope='session')
def ydb_local_table():
    params = _get_ydb_local_connection_params()
    table_name = YDB_LOCAL_TABLE_NAME
    table = ydb.TableDescription().with_columns(
        *[
            ydb.Column(column_name, ydb.OptionalType(value_type))
            for column_name, value_type in TABLE_DESCRIPTION.items()
        ]
    ).with_primary_key('id')

    connection_params = ydb.DriverConfig(
        endpoint=f"{params['host']}:{params['port']}",
        database=params['db_name'],
    )
    driver = ydb.Driver(connection_params)
    driver.wait(timeout=5)
    session = driver.table_client.session().create()
    table_path = os.path.join(params['db_name'], table_name)
    try:
        session.drop_table(table_path)
    except Exception:
        pass
    session.create_table(table_path, table)
    for sample_data_item in SAMPLE_DATA:
        session.transaction().execute(
            _replace_query(table_name, sample_data_item),
            commit_tx=True,
        )


@pytest.fixture(scope='function')
def ydb_local_table_dataset(request, client, api_v1, ydb_local_connection_id):
    ds = Dataset()
    ds.sources['source_1'] = ds.source(
        connection_id=ydb_local_connection_id,
        source_type=SOURCE_TYPE_YDB_TABLE,
        parameters=dict(
            table_name=YDB_LOCAL_TABLE_NAME,
        ),
    )
    ds.source_avatars['avatar_1'] = ds.sources['source_1'].avatar()
    ds = api_v1.apply_updates(dataset=ds).dataset
    ds_resp = api_v1.save_dataset(dataset=ds, preview=False)
    assert ds_resp.status_code == 200
    ds = ds_resp.dataset

    def teardown(ds_id=ds.id):
        client.delete('/api/v1/datasets/{}'.format(ds_id))

    request.addfinalizer(teardown)

    return ds


@pytest.fixture(scope='function')
def ydb_ext_connection_id_failed_authentication(app, client, request):
    return _make_ydb_connection_id_fixture(
        client,
        _get_ydb_ext_connection_params(token='AQAD-nevermind'),
        request,
    )


@pytest.fixture(scope='function')
def ydb_ext_connection_id_failed_authorization(app, client, request, yt_token):
    return _make_ydb_connection_id_fixture(
        client,
        _get_ydb_ext_connection_params(token=yt_token, db_name='/ru-prestable/home/hhell/mydb'),
        request,
    )
