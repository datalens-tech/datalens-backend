from dl_formula_testing.testcases.operators import DefaultOperatorFormulaConnectorTestSuite

from dl_connector_starrocks_tests.db.formula.base import StarRocksTestBase


class TestOperatorsStarRocks(StarRocksTestBase, DefaultOperatorFormulaConnectorTestSuite):
    subtraction_round_dt = False
