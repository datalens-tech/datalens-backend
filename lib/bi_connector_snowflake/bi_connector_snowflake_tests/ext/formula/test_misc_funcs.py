from bi_connector_snowflake_tests.ext.formula.base import SnowFlakeTestBase  # noqa
from bi_formula.connectors.base.testing.misc_funcs import (
    DefaultMiscFunctionalityConnectorTestSuite,
)


class TestMiscFunctionalitySnowFlake(SnowFlakeTestBase, DefaultMiscFunctionalityConnectorTestSuite):
    pass
