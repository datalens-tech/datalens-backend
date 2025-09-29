import sqlalchemy as sa

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_array import DefaultArrayFunctionFormulaConnectorTestSuite

from dl_connector_ydb_tests.db.formula.base import YQLTestBase


class ArrayFunctionYDBTestSuite(DefaultArrayFunctionFormulaConnectorTestSuite):
    make_decimal_cast = None
    make_float_cast = "Double?"
    make_float_array_cast = "List<Double?>?"
    make_str_array_cast = "List<String?>?"

    def test_startswith_string_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("STARTSWITH([arr_str_value], [arr_str_value])", from_=data_table)
        assert not dbe.eval(
            'STARTSWITH([arr_str_value], DB_CAST(ARRAY("", "cde", NULL), "varchar[]"))', from_=data_table
        )

    def test_array_contains_all_string_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('CONTAINS_ALL([arr_str_value], DB_CAST(ARRAY("cde"), "varchar[]"))', from_=data_table)
        assert dbe.eval('CONTAINS_ALL([arr_str_value], DB_CAST(ARRAY("cde", NULL), "varchar[]"))', from_=data_table)
        assert not dbe.eval('CONTAINS_ALL(DB_CAST(ARRAY("cde"), "varchar[]"), [arr_str_value])', from_=data_table)

    def test_array_contains_any_string_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('CONTAINS_ANY([arr_str_value], DB_CAST(ARRAY("cde"), "varchar[]"))', from_=data_table)
        assert dbe.eval('CONTAINS_ANY([arr_str_value], DB_CAST(ARRAY("123", NULL), "varchar[]"))', from_=data_table)
        assert dbe.eval('CONTAINS_ANY(DB_CAST(ARRAY("cde"), "varchar[]"), [arr_str_value])', from_=data_table)


class TestArrayFunctionYDB(YQLTestBase, ArrayFunctionYDBTestSuite):
    pass
