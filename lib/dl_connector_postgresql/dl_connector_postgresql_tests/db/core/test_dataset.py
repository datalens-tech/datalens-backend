from dl_core_testing.testcases.dataset import DefaultDatasetTestSuite
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_postgresql.core.postgresql.constants import SOURCE_TYPE_PG_TABLE
from dl_connector_postgresql.core.postgresql.us_connection import ConnectionPostgreSQL
from dl_connector_postgresql_tests.db.core.base import BasePostgreSQLTestClass


class TestPostgreSQLDataset(BasePostgreSQLTestClass, DefaultDatasetTestSuite[ConnectionPostgreSQL]):
    source_type = SOURCE_TYPE_PG_TABLE

    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultDatasetTestSuite.test_get_param_hash: "db_name in dsrc",  # TODO: FIXME
        },
    )
