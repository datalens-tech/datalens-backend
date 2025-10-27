import pytest
import sqlalchemy as sa

from dl_formula.core import exc
from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.base import FormulaConnectorTestBase


class DefaultNativeFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    @pytest.mark.parametrize(
        "threatening_function_name",
        [
            "delete tables",
            ";delete_tables",
            "some_call()",
        ],
    )
    def test_native_functions_validation(self, dbe: DbEvaluator, threatening_function_name: str) -> None:
        with pytest.raises(exc.TranslationError) as exc_info:
            dbe.eval(f"DB_CALL_INT('{threatening_function_name}')")

        assert exc_info.value.errors[0].code == exc.NativeFunctionForbiddenInputError.default_code

    @pytest.mark.parametrize(
        "func_return_type", ["INT", "FLOAT", "STRING", "BOOL", "ARRAY_INT", "ARRAY_FLOAT", "ARRAY_STRING"]
    )
    def test_native_functions_arg_type_validation(
        self, dbe: DbEvaluator, data_table: sa.Table, func_return_type: str
    ) -> None:
        func_name = f"DB_CALL_{func_return_type}"
        with pytest.raises(exc.TranslationError) as exc_info:
            dbe.eval(f"{func_name}([str_value])", from_=data_table)

        assert f"Invalid argument types for function {func_name}" in str(exc_info.value)


class DefaultNativeAggregationFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    pass
