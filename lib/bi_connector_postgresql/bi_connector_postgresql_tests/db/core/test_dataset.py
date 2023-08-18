from bi_connector_postgresql.core.postgresql.constants import SOURCE_TYPE_PG_TABLE
from bi_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL

from bi_connector_postgresql_tests.db.core.base import BasePostgreSQLTestClass

from bi_core_testing.testcases.dataset import DefaultDatasetTestSuite


class TestPostgreSQLDataset(BasePostgreSQLTestClass, DefaultDatasetTestSuite[ConnectionPostgreSQL]):
    source_type = SOURCE_TYPE_PG_TABLE
    do_check_param_hash = False  # FIXME: db_name in dsrc
