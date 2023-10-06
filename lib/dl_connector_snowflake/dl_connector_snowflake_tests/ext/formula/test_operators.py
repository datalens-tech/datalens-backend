from dl_formula_testing.testcases.operators import DefaultOperatorFormulaConnectorTestSuite

from dl_connector_snowflake_tests.ext.formula.base import SnowFlakeTestBase  # noqa


class TestOperatorSnowFlake(SnowFlakeTestBase, DefaultOperatorFormulaConnectorTestSuite):
    pass
