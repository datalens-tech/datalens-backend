import sqlalchemy as sa

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.operators import DefaultOperatorFormulaConnectorTestSuite

from dl_connector_ydb_tests.db.formula.base import YQLTestBase


class TestOperatorYQL(YQLTestBase, DefaultOperatorFormulaConnectorTestSuite):
    subtraction_round_dt = False
    supports_string_int_multiplication = False

    def test_subtraction_unsigned_ints(self, dbe: DbEvaluator) -> None:
        assert dbe.eval("SECOND(#2019-01-23 15:07:47#) - SECOND(#2019-01-23 15:07:48#)") == -1

    def test_in_date(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # YDB doesn't allow ordering by columns not from SELECT clause, so use WHERE instead
        assert dbe.eval(
            "[date_value] in (#2014-10-05#)", where="IF([date_value] = #2014-10-05#, TRUE, FALSE)", from_=data_table
        )
        assert dbe.eval(
            "[date_value] not in (#2014-10-05#)", where="IF([date_value] = #2014-10-06#, TRUE, FALSE)", from_=data_table
        )
