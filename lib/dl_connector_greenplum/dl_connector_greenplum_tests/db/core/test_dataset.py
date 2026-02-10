from dl_core_testing.testcases.dataset import DefaultDatasetTestSuite
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_greenplum.core.constants import SOURCE_TYPE_GP_TABLE
from dl_connector_greenplum.core.us_connection import GreenplumConnection
from dl_connector_greenplum_tests.db.core.base import (
    GP6TestClass,
    GP7TestClass,
)


class TestGP6Dataset(GP6TestClass, DefaultDatasetTestSuite[GreenplumConnection]):
    source_type = SOURCE_TYPE_GP_TABLE

    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultDatasetTestSuite.test_get_param_hash: "db_name in dsrc",  # TODO: FIXME
        },
    )


class TestGP7Dataset(GP7TestClass, DefaultDatasetTestSuite[GreenplumConnection]):
    source_type = SOURCE_TYPE_GP_TABLE

    test_params = RegulatedTestParams(
        mark_tests_failed={
            DefaultDatasetTestSuite.test_get_param_hash: "db_name in dsrc",  # TODO: FIXME
        },
    )
