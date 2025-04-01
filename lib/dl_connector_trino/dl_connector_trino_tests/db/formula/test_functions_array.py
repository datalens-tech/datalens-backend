import pytest
import sqlalchemy as sa

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_array import DefaultArrayFunctionFormulaConnectorTestSuite

from dl_connector_trino_tests.db.formula.base import TrinoFormulaTestBase


class TestArrayFunctionTrino(TrinoFormulaTestBase, DefaultArrayFunctionFormulaConnectorTestSuite):
    def test_unnest_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        pytest.skip("Not implemented")

    def test_startswith_string_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("STARTSWITH([arr_str_value], [arr_str_value])", from_=data_table)
        assert not dbe.eval('STARTSWITH([arr_str_value], ARRAY("", "cde", NULL))', from_=data_table)
