from bi_connector_snowflake_tests.ext.formula.base import SnowFlakeTestBase  # noqa
from bi_formula_testing.testcases.misc_funcs import (
    DefaultMiscFunctionalityConnectorTestSuite,
)


class TestMiscFunctionalitySnowFlake(SnowFlakeTestBase, DefaultMiscFunctionalityConnectorTestSuite):
    pass
