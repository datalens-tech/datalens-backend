from dl_formula_testing.testcases.operators import DefaultOperatorFormulaConnectorTestSuite

from dl_connector_starrocks_tests.db.formula.base import StarRocksTestBase


class TestOperatorsStarRocks(StarRocksTestBase, DefaultOperatorFormulaConnectorTestSuite):  # type: ignore  # 2024-01-30 # TODO: fix
    subtraction_round_dt = False
    supports_string_int_multiplication = True
