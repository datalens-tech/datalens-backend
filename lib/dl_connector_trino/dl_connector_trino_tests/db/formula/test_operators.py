from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.operators import DefaultOperatorFormulaConnectorTestSuite

from dl_connector_trino_tests.db.formula.base import TrinoFormulaTestBase


class TestOperatorTrino(TrinoFormulaTestBase, DefaultOperatorFormulaConnectorTestSuite):
    subtraction_round_dt = False

    # In Trino, arrays containing NULLs cannot be equal (result is either NULL or FALSE).
    # Therefore, we have to redefine this test.
    def test_comparison_operators(self, dbe: DbEvaluator) -> None:
        assert dbe.eval("1 < 3 <= 4 = 4 != 10 > 7 >= 6")
        assert dbe.eval('"qwerty" = "qwerty"')
        assert dbe.eval('"qwerty" != "abc"')
        assert dbe.eval('"qwerty" > "abc"')
        assert dbe.eval('"qwerty" >= "abc"')
        assert dbe.eval('"abc" < "qwerty"')
        assert dbe.eval('"abc" <= "qwerty"')
        if self.supports_arrays:
            assert dbe.eval("ARRAY(1, 2, 4) = ARRAY(1, 2, 4)")
            assert dbe.eval("ARRAY(1, 2, 4) = ARRAY(1, 2, 3)") == False
            assert dbe.eval("ARRAY(1, 2, NULL, 4) = ARRAY(1, 2, NULL, 4)") is None
            assert dbe.eval("ARRAY(1, 2, 4) != ARRAY(1, 2, 3)")
            assert dbe.eval("ARRAY(1, 2, 4) != ARRAY(1, 2, 4)") == False
            assert dbe.eval("ARRAY(1, 2, NULL, 4) != ARRAY(1, 2, NULL, 4)") is None
            assert dbe.eval("ARRAY(1, 2, 4) != ARRAY(1, 2, NULL, 4)")
