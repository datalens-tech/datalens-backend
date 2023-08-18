from bi_connector_mssql.core.constants import SOURCE_TYPE_MSSQL_TABLE
from bi_connector_mssql.core.us_connection import ConnectionMSSQL

from bi_connector_mssql_tests.db.core.base import BaseMSSQLTestClass

from bi_core_testing.testcases.dataset import DefaultDatasetTestSuite


class TestMSSQLDataset(BaseMSSQLTestClass, DefaultDatasetTestSuite[ConnectionMSSQL]):
    source_type = SOURCE_TYPE_MSSQL_TABLE
    do_check_param_hash = False  # FIXME: db_name in dsrc
