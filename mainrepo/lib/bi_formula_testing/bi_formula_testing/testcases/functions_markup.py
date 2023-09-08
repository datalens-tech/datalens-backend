from __future__ import annotations

from typing import Any

import pytest

import bi_formula.core.exc as exc
from bi_formula_testing.testcases.base import FormulaConnectorTestBase
from bi_formula_testing.evaluator import DbEvaluator
from bi_formula_testing.util import to_str


class DefaultMarkupFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    def test_markup_functions_simple(self, dbe: DbEvaluator) -> None:
        # markup value is a string, but it shouldn't normally leak to the user.
        with pytest.raises(exc.TranslationError):
            dbe.eval("str(italic('text00'))")

        with pytest.raises(exc.TranslationError):
            dbe.eval("str(italic('text00') + 'text01')")

        with pytest.raises(exc.TranslationError):
            dbe.eval("__str(italic('text00') + 'text01')")

        assert to_str(dbe.eval("markup(italic('text00'), bold('text01'))")) == '(c (i "text00") (b "text01"))'
        assert to_str(dbe.eval("markup(italic('text00'), 'text01')")) == '(c (i "text00") "text01")'

        # TODO: Decide the desired behavior.
        assert (dbe.eval("'abc' + NULL + 'qwe'") is None) == (dbe.eval("italic(NULL) + '...'") is None)

        assert to_str(dbe.eval("italic('text00') + 'text01'")) == '(c (i "text00") "text01")'
        assert to_str(dbe.eval("italic('text0')")) == '(i "text0")'

        assert to_str(dbe.eval("italic('text1') + bold('text2')")) == '(c (i "text1") (b "text2"))'
        assert to_str(dbe.eval("""italic('text "3"')""")) == '(i "text ""3""")'
        assert to_str(dbe.eval("url('http://example.com', 'text4')")) == '(a "http://example.com" "text4")'
        assert to_str(dbe.eval("url('javascript:alert(5)', bold('text5'))")) == '(a "javascript:alert(5)" (b "text5"))'

    def test_markup_functions_lit(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        expr = """
        italic(__LIT__('text "6"') + ' ... ') +
        __LIT__('text "8"') + ', text "9"'
        """
        expected_best_case = (
            '(c '
            '(i "text ""6"" ...") '
            '"text ""8""" ", text ""9""")')
        expected_acceptable = (
            '(c '
            '(c (i "text ""6"" ... ") "text ""8""") '
            '", text ""9""")')
        assert to_str(dbe.eval(expr)) in (expected_best_case, expected_acceptable)
