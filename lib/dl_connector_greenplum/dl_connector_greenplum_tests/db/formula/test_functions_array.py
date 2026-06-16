import pytest
import sqlalchemy as sa

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_array import DefaultArrayFunctionFormulaConnectorTestSuite
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_greenplum_tests.db.formula.base import GreenplumTestBase


GP_ROW_ORDER_SKIP_REASON = "Greenplum row order is non-deterministic without ORDER BY"


class ArrayFunctionGreenplumTestSuite(DefaultArrayFunctionFormulaConnectorTestSuite):
    make_decimal_cast = "numeric"
    make_float_cast = "double precision"
    make_float_array_cast = "double precision[]"
    make_str_array_cast = "text[]"

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


class TestArrayFunctionGreenplum(GreenplumTestBase, ArrayFunctionGreenplumTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            # These tests depend on specific row values which are non-deterministic in Greenplum
            DefaultArrayFunctionFormulaConnectorTestSuite.test_create_array: GP_ROW_ORDER_SKIP_REASON,
            DefaultArrayFunctionFormulaConnectorTestSuite.test_array_str: GP_ROW_ORDER_SKIP_REASON,
            DefaultArrayFunctionFormulaConnectorTestSuite.test_array_count_item_int: GP_ROW_ORDER_SKIP_REASON,
            DefaultArrayFunctionFormulaConnectorTestSuite.test_array_count_item_float: GP_ROW_ORDER_SKIP_REASON,
            DefaultArrayFunctionFormulaConnectorTestSuite.test_array_contains_column: GP_ROW_ORDER_SKIP_REASON,
            DefaultArrayFunctionFormulaConnectorTestSuite.test_array_contains_all_column: GP_ROW_ORDER_SKIP_REASON,
            DefaultArrayFunctionFormulaConnectorTestSuite.test_array_contains_any_column: GP_ROW_ORDER_SKIP_REASON,
            DefaultArrayFunctionFormulaConnectorTestSuite.test_array_not_contains_column: GP_ROW_ORDER_SKIP_REASON,
            DefaultArrayFunctionFormulaConnectorTestSuite.test_replace_array_column: GP_ROW_ORDER_SKIP_REASON,
            DefaultArrayFunctionFormulaConnectorTestSuite.test_array_cast: GP_ROW_ORDER_SKIP_REASON,
            DefaultArrayFunctionFormulaConnectorTestSuite.test_array_remove: GP_ROW_ORDER_SKIP_REASON,
            DefaultArrayFunctionFormulaConnectorTestSuite.test_array_intersection: GP_ROW_ORDER_SKIP_REASON,
            DefaultArrayFunctionFormulaConnectorTestSuite.test_array_index_of_int: GP_ROW_ORDER_SKIP_REASON,
            DefaultArrayFunctionFormulaConnectorTestSuite.test_array_index_of_float: GP_ROW_ORDER_SKIP_REASON,
            DefaultArrayFunctionFormulaConnectorTestSuite.test_array_slice: GP_ROW_ORDER_SKIP_REASON,
            DefaultArrayFunctionFormulaConnectorTestSuite.test_array_get_item_nested: GP_ROW_ORDER_SKIP_REASON,
        },
    )
