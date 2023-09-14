from __future__ import annotations

import pytest
import sqlalchemy as sa

from bi_formula.core.dialect import from_name_and_version
import bi_formula.core.exc as exc
from bi_formula.shortcuts import n
from bi_formula.translation.ext_nodes import CompiledExpression
from bi_formula.translation.translator import translate
from bi_formula_testing.evaluator import DbEvaluator
from bi_formula_testing.testcases.base import FormulaConnectorTestBase


class DefaultMiscFunctionalityConnectorTestSuite(FormulaConnectorTestBase):
    def test_dialect_resolution(self, dbe: DbEvaluator) -> None:
        assert from_name_and_version(self.dialect.common_name, dbe.db.get_version()) == dbe.dialect

    def test_translate_for_where(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        # WHERE already is a condition
        assert dbe.eval("MIN([int_value])", from_=data_table, where="[int_value] > 30") == 40
        # WHERE is a boolean value, might have to be converted to a condition
        assert dbe.eval("MIN([int_value])", from_=data_table, where="IF([int_value] > 30, TRUE, FALSE)") == 40

    def test_translation_recombination(self, dbe: DbEvaluator) -> None:
        # In regular circumstances such a formula raises an error,
        # but here its parts are translated separately and then combined into a single expression.
        # Should still raise the same error
        dialect = self.dialect
        with pytest.raises(exc.TranslationError):
            translate(
                n.formula(
                    n.func.IF(
                        CompiledExpression.make(
                            translate(
                                n.formula(n.func.WHAA(n.field("n1"))),
                                dialect=dialect,
                            )
                        ),
                        CompiledExpression.make(
                            translate(
                                n.formula(n.field("n2")),
                                dialect=dialect,
                            )
                        ),
                        n.lit(42),
                    )
                ),
                dialect=dialect,
            )
