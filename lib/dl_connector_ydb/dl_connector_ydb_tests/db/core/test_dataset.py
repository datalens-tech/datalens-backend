from dl_core_testing.testcases.dataset import DefaultDatasetTestSuite
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_ydb.core.ydb.constants import SOURCE_TYPE_YDB_TABLE
from dl_connector_ydb.core.ydb.us_connection import YDBConnection
from dl_connector_ydb_tests.db.core.base import BaseYDBTestClass


class TestYDBSQLDataset(BaseYDBTestClass, DefaultDatasetTestSuite[YDBConnection]):
    source_type = SOURCE_TYPE_YDB_TABLE

    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultDatasetTestSuite.test_get_param_hash: "db_name in dsrc",  # TODO: FIXME
        },
    )
