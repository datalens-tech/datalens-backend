from dl_formula_testing.testcases.literals import DefaultLiteralFormulaConnectorTestSuite

from dl_connector_greenplum_tests.db.formula.base import GreenplumTestBase


class LiteralGreenplumTestSuite(DefaultLiteralFormulaConnectorTestSuite):
    supports_microseconds = True
    supports_utc = True
    supports_custom_tz = True


class TestLiteralGreenplum(GreenplumTestBase, LiteralGreenplumTestSuite):
    pass
