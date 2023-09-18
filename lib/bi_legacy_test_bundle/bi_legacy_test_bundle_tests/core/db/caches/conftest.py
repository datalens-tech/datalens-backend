from __future__ import annotations

import pytest
from sqlalchemy.engine.url import make_url

from dl_constants.enums import BIType, RawSQLLevel

from dl_core_testing.connection import make_saved_connection
from dl_core_testing.database import make_db, make_table, C
from dl_core_testing.dataset import make_dataset

from dl_connector_postgresql.core.postgresql.constants import CONNECTION_TYPE_POSTGRES


@pytest.fixture(scope='session')
def db_def_cache_tests_pg(postgres_db):
    db_name = 'caches_tests'
    conn = postgres_db.get_current_connection()

    conn.execute('COMMIT')
    conn.execute('DROP DATABASE IF EXISTS {}'.format(db_name))
    conn.execute('COMMIT')
    conn.execute("CREATE DATABASE {}".format(db_name))

    new_url = make_url(str(postgres_db.url))
    new_url = new_url.set(database=db_name)

    db = make_db(url=new_url, conn_type=CONNECTION_TYPE_POSTGRES, )
    yield db


@pytest.fixture(scope='function')
def us_conn_cache_tests_pg(db_def_cache_tests_pg, default_sync_usm):
    return make_saved_connection(
        sync_usm=default_sync_usm,
        db=db_def_cache_tests_pg,
        data_dict=dict(raw_sql_level=RawSQLLevel.dashsql),
    )


@pytest.fixture(scope='session')
def table_for_cached_dataset(db_def_cache_tests_pg):
    tbl = make_table(db_def_cache_tests_pg, rows=int(1e2), columns=[
        C('id', BIType.integer, vg=lambda rn, **kwargs: rn),
        C('val', BIType.string, vg=lambda rn, **kwargs: 'ST:{}'.format(rn)),
    ])
    yield tbl
    db_def_cache_tests_pg.drop_table(tbl.table)


@pytest.fixture(scope='function')
def cached_dataset_postgres(us_conn_cache_tests_pg, default_sync_usm, db_def_cache_tests_pg, table_for_cached_dataset):
    dataset = make_dataset(
        sync_usm=default_sync_usm,
        connection=us_conn_cache_tests_pg,
        db_table=table_for_cached_dataset,
    )
    default_sync_usm.save(dataset)
    yield dataset
    default_sync_usm.delete(dataset)
