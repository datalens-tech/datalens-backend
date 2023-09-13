from __future__ import annotations

import enum
import uuid

import aiobotocore.client
import pytest
import sqlalchemy as sa

import bi_legacy_test_bundle_tests.core.config as tests_config_mod
from bi_configs.settings_submodels import S3Settings
from bi_constants.enums import (
    ConnectionType,
    DataSourceCreatedVia,
    RawSQLLevel,
)
from bi_core import exc

from bi_connector_clickhouse.core.constants import (
    CONNECTION_TYPE_CLICKHOUSE,
    SOURCE_TYPE_CH_SUBSELECT,
)
from bi_connector_postgresql.core.postgresql_base.constants import PGEnforceCollateMode
from bi_connector_bundle_chs3.chs3_base.core.us_connection import BaseFileS3Connection
from bi_connector_bundle_chs3.chs3_gsheets.core.testing.connection import make_saved_gsheets_v2_connection
from bi_connector_bundle_chs3.file.core.testing.connection import make_saved_file_connection
from bi_core_testing.connection import make_saved_connection
from bi_core_testing.connector import CONNECTION_TYPE_TESTING
from bi_core_testing.database import (
    Db, DbTable, make_table, make_multiple_db_tables, make_schema,
    make_view_from_table, CoreReInitableDbDispenser, make_db_config,
)
from bi_core_testing.dataset import make_dataset
from bi_core_testing.environment import common_pytest_configure, prepare_united_storage_from_config
from bi_connector_bundle_chs3.chs3_base.core.testing.utils import create_s3_native_from_ch_table
from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_testing.s3_utils import create_s3_client, create_s3_bucket, s3_tbl_func_maker
from bi_testing_ya.sql_queries import CH_QUERY_FULL
from clickhouse_sqlalchemy import types as ch_types

from bi_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL
from bi_connector_mysql.core.constants import CONNECTION_TYPE_MYSQL
from bi_connector_oracle.core.constants import CONNECTION_TYPE_ORACLE
from bi_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES
from bi_connector_bundle_chs3.chs3_gsheets.core.constants import CONNECTION_TYPE_GSHEETS_V2, SOURCE_TYPE_GSHEETS_V2
from bi_connector_bundle_chs3.file.core.constants import CONNECTION_TYPE_FILE, SOURCE_TYPE_FILE_S3_TABLE

from bi_legacy_test_bundle_tests.core.conftest import clear_logging_context, loaded_libraries  # noqa: F401


def pytest_configure(config):  # noqa
    common_pytest_configure()
    prepare_united_storage_from_config(us_config=tests_config_mod.CORE_TEST_CONFIG.get_us_config())


# ####### Database definitions #######


DB_CONFIGURATIONS = {
    ConnectionType[name]: url
    for name, url in tests_config_mod.DB_CONFIGURATIONS.items()
    if getattr(ConnectionType, name, None) is not None
}
_SORTED_CONFIGS = sorted(DB_CONFIGURATIONS.items(), key=lambda item: item[0].value)


DEFAULT_SCHEMAS = {
    CONNECTION_TYPE_POSTGRES: 'public',
    CONNECTION_TYPE_MSSQL: 'dbo',
    CONNECTION_TYPE_ORACLE: 'DATALENS',
}
SCHEMATIZED_DB = {
    CONNECTION_TYPE_POSTGRES,
    CONNECTION_TYPE_MSSQL,
    CONNECTION_TYPE_ORACLE,
}


@pytest.fixture(scope='function', autouse=True)
def reinit_dbs(db_dispenser) -> None:
    assert isinstance(db_dispenser, CoreReInitableDbDispenser)
    db_dispenser.check_reinit_all()


@pytest.fixture(
    scope='session',
    params=[(url, conn_type) for conn_type, url in _SORTED_CONFIGS],
    ids=[conn_type.value for conn_type, _ in _SORTED_CONFIGS]
)
def db(request, initdb_ready, db_dispenser) -> Db:
    url, cluster = request.param[0]
    if not url:
        raise pytest.skip(f'db: No db for {request.param[1].value!r}')
    return db_dispenser.get_database(
        db_config=make_db_config(url=url, conn_type=request.param[1], cluster=cluster),
    )


@pytest.fixture(
    scope='session',
    params=[(url, conn_type) for conn_type, url in _SORTED_CONFIGS if conn_type in SCHEMATIZED_DB],
    ids=[conn_type.value for conn_type, _ in _SORTED_CONFIGS if conn_type in SCHEMATIZED_DB]
)
def schematized_db(request, db_dispenser) -> Db:
    url, cluster = request.param[0]
    if not url:
        raise pytest.skip(f'schematized_db: No db for {request.param[1].value!r}')
    return db_dispenser.get_database(
        db_config=make_db_config(url=url, conn_type=request.param[1], cluster=cluster),
    )


def _db_for(conn_type: ConnectionType, db_dispenser, bi_config=None) -> Db:
    if bi_config is not None:
        url, cluster = bi_config.DB_CONFIGURATIONS[conn_type.value]
    else:
        url, cluster = DB_CONFIGURATIONS[conn_type]
    if not url:
        # return None
        raise pytest.skip(f'db_for: No db for {conn_type!r}')
    return db_dispenser.get_database(
        db_config=make_db_config(url=url, conn_type=conn_type, cluster=cluster),
    )


# ####### S3 & file-uploader stuff #######


@pytest.fixture(scope='session')
def s3_settings() -> S3Settings:
    return S3Settings(
        ENDPOINT_URL=f"http://{tests_config_mod.get_test_container_hostport('s3-storage', fallback_port=51620).as_pair()}",
        ACCESS_KEY_ID='accessKey1',
        SECRET_ACCESS_KEY='verySecretKey1'
    )


@pytest.fixture()
def s3_tbl_func(s3_settings):
    return s3_tbl_func_maker(s3_settings)


@pytest.fixture(scope='function')
async def s3_client(s3_settings) -> aiobotocore.client.AioBaseClient:
    async with create_s3_client(s3_settings) as client:
        yield client


@pytest.fixture(scope='function')
async def s3_bucket(s3_client) -> str:
    bucket_name = 'bi-file-uploader'
    await create_s3_bucket(s3_client, bucket_name)
    yield bucket_name


@pytest.fixture()
async def s3_native_from_ch_table(s3_client, s3_bucket, s3_settings, clickhouse_table):
    filename = 'my_file.native'
    tbl_schema = ("string_value Nullable(String), n_string_value Nullable(String), int_value Nullable(Int64), "
                  "n_int_value Nullable(Int64), float_value Nullable(Float64), datetime_value Nullable(DateTime), "
                  "n_datetime_value Nullable(DateTime), date_value Nullable(Date), boolean_value Nullable(UInt8), "
                  "uuid_value Nullable(UUID)")  # TODO: update DbTable to serve some sort of schema
    create_s3_native_from_ch_table(filename, s3_bucket, s3_settings, clickhouse_table, tbl_schema)

    yield filename

    await s3_client.delete_object(Bucket=s3_bucket, Key=filename)


# ####### Databases #######


@pytest.fixture(scope='session')
def clickhouse_db(request, bi_config, db_dispenser) -> Db:
    return _db_for(CONNECTION_TYPE_CLICKHOUSE, bi_config=bi_config, db_dispenser=db_dispenser)


@pytest.fixture(scope='session')
def postgres_db(request, bi_config, db_dispenser) -> Db:
    return _db_for(CONNECTION_TYPE_POSTGRES, bi_config=bi_config, db_dispenser=db_dispenser)


@pytest.fixture(scope='session')
def postgres_db_fresh(request, bi_config, db_dispenser) -> Db:
    conn_type = CONNECTION_TYPE_POSTGRES
    url, cluster = bi_config.DB_CONFIGURATIONS['postgres_fresh']
    return db_dispenser.get_database(
        db_config=make_db_config(url=url, conn_type=conn_type, cluster=cluster),
    )


@pytest.fixture(scope='session')
def mysql_db(request, bi_config, db_dispenser) -> Db:
    return _db_for(CONNECTION_TYPE_MYSQL, bi_config=bi_config, db_dispenser=db_dispenser)


@pytest.fixture(scope='session')
def mssql_db(request, bi_config, db_dispenser) -> Db:
    return _db_for(CONNECTION_TYPE_MSSQL, bi_config=bi_config, db_dispenser=db_dispenser)


@pytest.fixture(scope='session')
def oracle_db(request, bi_config, db_dispenser) -> Db:
    return _db_for(CONNECTION_TYPE_ORACLE, bi_config=bi_config, db_dispenser=db_dispenser)


# ####### Tables #######


@pytest.fixture(scope='session')
def custom_clickhouse_table(clickhouse_db, request):
    enum_values = ["allchars '\"\t,= etc", "test", "value1"]
    my_enum8 = enum.Enum('MyEnum8', enum_values)
    my_enum16 = enum.Enum('MyEnum16', enum_values)
    # # Maybe (but will take obscenely long):
    # my_enum16 = enum.Enum('MyEnum16', ['value{}'.format(idx) for idx in range(65530)], start=-32766)
    table = clickhouse_db.table_from_columns([
        sa.Column(name='my_int_8', type_=ch_types.Nullable(ch_types.Int8())),
        sa.Column(name='my_int_16', type_=ch_types.Nullable(ch_types.Int16())),
        sa.Column(name='my_int_32', type_=ch_types.Nullable(ch_types.Int32())),
        sa.Column(name='my_int_64', type_=ch_types.Nullable(ch_types.Int64())),
        sa.Column(name='my_uint_8', type_=ch_types.Nullable(ch_types.UInt8())),
        sa.Column(name='my_uint_16', type_=ch_types.Nullable(ch_types.UInt16())),
        sa.Column(name='my_uint_32', type_=ch_types.Nullable(ch_types.UInt32())),
        sa.Column(name='my_uint_64', type_=ch_types.Nullable(ch_types.UInt64())),
        sa.Column(name='my_float_32', type_=ch_types.Nullable(ch_types.Float32())),
        sa.Column(name='my_float_64', type_=ch_types.Nullable(ch_types.Float64())),
        sa.Column(name='my_string', type_=ch_types.Nullable(ch_types.String())),
        # Note: `Nullable(LowCardinality(String))` is actually not allowed:
        # "Nested type LowCardinality(String) cannot be inside Nullable type"
        # Note: requires a fresh enough clickhouse (or a setting `allow_experimental_low_cardinality_type = 1`).
        sa.Column(name='my_string_lowcardinality', type_=ch_types.LowCardinality(ch_types.Nullable(ch_types.String()))),
        sa.Column(name='my_string_lowcardinality_nn', type_=ch_types.LowCardinality(ch_types.String())),
        sa.Column(name='my_enum8', type_=ch_types.Enum8(my_enum8)),
        sa.Column(name='my_enum16', type_=ch_types.Enum16(my_enum16)),
        sa.Column(name='my_date', type_=ch_types.Date()),  # not nullable so we can check 0000-00-00
        sa.Column(name='my_date32', type_=ch_types.Date32()),  # not nullable so we can check 0000-00-00
        sa.Column(name='my_datetime', type_=ch_types.DateTime()),  # not nullable so we can check 0000-00-00 00:00:00
        sa.Column(name='my_datetimewithtz', type_=ch_types.DateTimeWithTZ('Europe/Moscow')),
        sa.Column(name='my_datetime64', type_=ch_types.DateTime64(6)),  # same comment as above
        sa.Column(name='my_datetime64withtz', type_=ch_types.DateTime64WithTZ(6, 'Europe/Moscow')),
        sa.Column(name='my_decimal', type_=ch_types.Decimal(precision=8, scale=4)),
        sa.Column(name='my_bool', type_=ch_types.Nullable(ch_types.Bool())),
    ])
    clickhouse_db.create_table(table)
    return DbTable(db=clickhouse_db, table=table)


@pytest.fixture(scope='session')
def db_table(db):
    """Basic table for all db types"""
    return make_table(db, schema=DEFAULT_SCHEMAS.get(db.conn_type))


@pytest.fixture(scope='session')
def db_table_another_schema(db):
    """Basic table with another schema for all db types"""
    if db.conn_type not in [CONNECTION_TYPE_MSSQL, CONNECTION_TYPE_POSTGRES, CONNECTION_TYPE_ORACLE]:
        return None
    return make_table(db, schema=make_schema(db))


@pytest.fixture(scope='session')
def schematized_db_table(schematized_db):
    """Basic table for all db types"""
    return make_table(schematized_db, schema=make_schema(db=schematized_db))


@pytest.fixture(scope='session')
def db_view(db_table):
    """Basic view for all db types"""
    return make_view_from_table(db_table=db_table)


@pytest.fixture(scope='session')
def schematized_db_view(schematized_db_table):
    """Basic view for all db types"""
    return make_view_from_table(db_table=schematized_db_table)


@pytest.fixture(scope='session')
def multiple_db_tables(db):
    """Several logically linked tables for all db types"""
    return make_multiple_db_tables(db)


@pytest.fixture(scope='session')
def clickhouse_table(clickhouse_db):
    """Basic table for ClickHouse"""
    return make_table(clickhouse_db)


@pytest.fixture(scope='session')
def two_clickhouse_tables(clickhouse_db):
    """Two basic table for ClickHouse"""
    return make_table(clickhouse_db), make_table(clickhouse_db)


@pytest.fixture(scope='session')
def two_tables(db):
    """Two basic table for all databases"""
    return make_table(db), make_table(db)


@pytest.fixture(scope='session')
def multiple_clickhouse_tables(clickhouse_db):
    """Several logically linked tables for ClickHouse"""
    return make_multiple_db_tables(clickhouse_db)


@pytest.fixture(scope='session')
def geolayer_clickhouse_table(clickhouse_db):
    """Basic table for ClickHouse geo filtered connection"""
    return make_table(db=clickhouse_db, name='geolayer_table_1')


@pytest.fixture(scope='session')
def pg_partitioned_table(postgres_db_fresh):
    db = postgres_db_fresh
    name = 'test_partitioned_table_{}'.format(uuid.uuid4().hex)
    queries = [
        f'''
            create table {name}
            (ts timestamptz not null, value text)
            partition by range(ts)
        ''',
        f'''
            create table {name}_01
            partition of {name}
            for values from ('2020-01-01 00:00:00') to ('2021-01-01 00:00:00')
        ''',
        f'''
            insert into {name} (ts, value)
            values ('2020-01-01 01:02:03', 'a')
        ''',
    ]
    for query in queries:
        db.execute(query)
    yield name
    db.execute(f'drop table if exists {name}')


# ####### Connections #######


@pytest.fixture(scope='function')
def saved_testing_connection(default_sync_usm):
    conn = make_saved_connection(default_sync_usm, conn_type=CONNECTION_TYPE_TESTING)
    yield conn
    default_sync_usm.delete(conn)


@pytest.fixture(scope='function')
def saved_connection(default_sync_usm, db, app_context):
    conn = make_saved_connection(default_sync_usm, db)
    yield conn
    try:
        default_sync_usm.delete(conn)
    except exc.USObjectNotFoundException:
        pass


@pytest.fixture(scope='function')
def testlocal_saved_connection(default_sync_usm, postgres_db):
    conn = make_saved_connection(default_sync_usm, postgres_db)
    yield conn
    default_sync_usm.delete(conn)


@pytest.fixture(scope='function')
def saved_file_connection(default_sync_usm, clickhouse_table, s3_native_from_ch_table):
    conn = make_saved_file_connection(default_sync_usm, clickhouse_table, s3_native_from_ch_table)
    yield conn
    default_sync_usm.delete(conn)


@pytest.fixture(scope='function')
def saved_gsheets_v2_connection(default_sync_usm, clickhouse_table, s3_native_from_ch_table):
    conn = make_saved_gsheets_v2_connection(default_sync_usm, clickhouse_table, s3_native_from_ch_table)
    yield conn
    default_sync_usm.delete(conn)


@pytest.fixture(
    scope='function',
    params=['saved_file_connection', 'saved_gsheets_v2_connection'],
    ids=['file', 'gsheets'],
)
def saved_chs3_connection(request, default_sync_usm, clickhouse_table, s3_native_from_ch_table):
    conn = request.getfixturevalue(request.param)
    yield conn


@pytest.fixture(scope='function')
def another_saved_connection(default_sync_usm, db, app_context):
    conn = make_saved_connection(default_sync_usm, db)
    yield conn
    default_sync_usm.delete(conn)


@pytest.fixture(scope='function')
def saved_schematized_connection(default_sync_usm, schematized_db):
    conn = make_saved_connection(default_sync_usm, schematized_db)
    yield conn
    default_sync_usm.delete(conn)


@pytest.fixture(scope='function')
def saved_ch_connection(default_sync_usm, clickhouse_db, app_context):
    conn = make_saved_connection(default_sync_usm, clickhouse_db)
    yield conn
    default_sync_usm.delete(conn)


@pytest.fixture(scope='function')
def saved_pg_connection(default_sync_usm, postgres_db, app_context):
    conn = make_saved_connection(default_sync_usm, postgres_db)
    yield conn
    default_sync_usm.delete(conn)


@pytest.fixture(scope='function')
def saved_pg_fresh_connection(default_sync_usm, postgres_db_fresh, app_context):
    conn = make_saved_connection(default_sync_usm, postgres_db_fresh)
    yield conn
    default_sync_usm.delete(conn)


@pytest.fixture(scope='function')
def saved_pg_enforce_collate_connection(default_sync_usm, postgres_db, app_context):
    conn = make_saved_connection(
        default_sync_usm, postgres_db,
        data_dict=dict(
            enforce_collate=PGEnforceCollateMode.on,
        ),
    )
    yield conn
    default_sync_usm.delete(conn)


# ####### Datasets #######


@pytest.fixture(scope='function')
def saved_dataset(default_sync_usm, db_table, saved_connection, app_context):
    dataset = make_dataset(sync_usm=default_sync_usm, db_table=db_table, connection=saved_connection)
    default_sync_usm.save(dataset)
    yield dataset
    default_sync_usm.delete(dataset)


@pytest.fixture(scope='function')
def yt_to_dl_dataset(default_sync_usm, db_table, saved_connection, app_context):
    dataset = make_dataset(
        sync_usm=default_sync_usm,
        db_table=db_table,
        connection=saved_connection,
        created_via=DataSourceCreatedVia.yt_to_dl)
    default_sync_usm.save(dataset)
    yield dataset
    default_sync_usm.delete(dataset)


@pytest.fixture(scope='function')
def saved_dataset_per_func(default_sync_usm, db_table, saved_connection, app_context):
    dataset = make_dataset(sync_usm=default_sync_usm, db_table=db_table, connection=saved_connection)
    default_sync_usm.save(dataset)
    yield dataset
    default_sync_usm.delete(dataset)


@pytest.fixture(scope='function')
def saved_schematized_dataset(default_sync_usm, schematized_db_table, saved_schematized_connection):
    us_manager = default_sync_usm
    dataset = make_dataset(
        sync_usm=us_manager, db_table=schematized_db_table,
        connection=saved_schematized_connection,
    )
    us_manager.save(dataset)
    yield dataset
    us_manager.delete(dataset)


@pytest.fixture(scope='function')
def saved_dataset_for_view(default_sync_usm, db_view, saved_connection):
    us_manager = default_sync_usm
    dataset = make_dataset(
        sync_usm=us_manager, db_table=db_view.as_table,
        connection=saved_connection
    )
    us_manager.save(dataset)
    yield dataset
    us_manager.delete(dataset)


@pytest.fixture(scope='function')
def saved_schematized_dataset_for_view(default_sync_usm, schematized_db_view, saved_schematized_connection):
    us_manager = default_sync_usm
    dataset = make_dataset(
        sync_usm=us_manager, db_table=schematized_db_view.as_table,
        connection=saved_schematized_connection,
    )
    us_manager.save(dataset)
    yield dataset
    us_manager.delete(dataset)


@pytest.fixture(scope='function')
def saved_dataset_no_dsrc(default_sync_usm, app_context):
    dataset = make_dataset(sync_usm=default_sync_usm)
    default_sync_usm.save(dataset)
    yield dataset
    default_sync_usm.delete(dataset)


@pytest.fixture
def saved_pg_enforce_collate_dataset(default_sync_usm, postgres_db, saved_pg_enforce_collate_connection):
    db = postgres_db
    conn = saved_pg_enforce_collate_connection
    db_table = make_table(db, schema=DEFAULT_SCHEMAS.get(db.conn_type))
    dataset = make_dataset(sync_usm=default_sync_usm, db_table=db_table, connection=conn)
    default_sync_usm.save(dataset)
    yield dataset
    default_sync_usm.delete(dataset)


@pytest.fixture(scope='function')
def saved_ch_subselectable_connection(default_sync_usm, clickhouse_db, app_context):
    conn = make_saved_connection(
        default_sync_usm, clickhouse_db,
        data_dict=dict(raw_sql_level=RawSQLLevel.dashsql),
    )
    yield conn
    default_sync_usm.delete(conn)


@pytest.fixture(scope='function')
def saved_ch_subselect_dataset(default_sync_usm: SyncUSManager, saved_ch_subselectable_connection, app_context):
    conn = saved_ch_subselectable_connection
    # Note: `7` there results in 7 rows which is checked in a further test.
    query = CH_QUERY_FULL
    dataset = make_dataset(
        sync_usm=default_sync_usm,
        connection=conn,
        created_from=SOURCE_TYPE_CH_SUBSELECT,
        dsrc_params=dict(
            subsql=query,
        ),
    )
    default_sync_usm.save(dataset)
    yield dataset
    default_sync_usm.delete(dataset)


@pytest.fixture(scope='function')
def saved_ch_dataset(default_sync_usm: SyncUSManager, clickhouse_table, saved_ch_connection, app_context):
    dataset = make_dataset(
        default_sync_usm,
        db_table=clickhouse_table,
        connection=saved_ch_connection,
    )
    default_sync_usm.save(dataset)
    yield dataset
    default_sync_usm.delete(dataset)


@pytest.fixture(scope='function')
def saved_ch_dataset_per_func(default_sync_usm, clickhouse_table, saved_ch_connection, app_context):
    dataset = make_dataset(
        default_sync_usm,
        db_table=clickhouse_table,
        connection=saved_ch_connection,
    )
    default_sync_usm.save(dataset)
    yield dataset
    default_sync_usm.delete(dataset)


@pytest.fixture(scope='function')
def saved_chs3_dataset(default_sync_usm, clickhouse_table, saved_chs3_connection: BaseFileS3Connection, app_context):
    conn_type_to_source_type_map = {
        CONNECTION_TYPE_FILE: SOURCE_TYPE_FILE_S3_TABLE,
        CONNECTION_TYPE_GSHEETS_V2: SOURCE_TYPE_GSHEETS_V2,
    }
    dataset = make_dataset(
        default_sync_usm,
        connection=saved_chs3_connection,
        created_from=conn_type_to_source_type_map[saved_chs3_connection.conn_type],
        dsrc_params=dict(origin_source_id=saved_chs3_connection.data.sources[0].id),
    )
    default_sync_usm.save(dataset)
    yield dataset
    default_sync_usm.delete(dataset)


@pytest.fixture(scope='function')
def testlocal_saved_dataset(default_sync_usm, testlocal_saved_connection, postgres_db, app_context):
    db_table = make_table(postgres_db)
    dataset = make_dataset(
        default_sync_usm,
        testlocal_saved_connection,
        db_table=db_table,
    )
    default_sync_usm.save(dataset)
    yield dataset
    default_sync_usm.delete(dataset)
