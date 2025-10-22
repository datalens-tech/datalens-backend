import pytest

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
