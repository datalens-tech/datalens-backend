from __future__ import annotations

from typing import (
    Any,
    ClassVar,
)

import pytest

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.base import FormulaConnectorTestBase
from dl_formula_testing.util import to_str


class DefaultLogicalFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    supports_nan_funcs: ClassVar[bool] = False
    supports_iif: ClassVar[bool] = False

    def test_isnull(self, dbe: DbEvaluator) -> None:
        assert dbe.eval("ISNULL(NULL)")
        assert not dbe.eval("ISNULL(2)")

    def test_ifnull(self, dbe: DbEvaluator) -> None:
        assert dbe.eval("IFNULL(1, 2)") == 1
        assert dbe.eval("IFNULL(NULL, 2)") == 2
        assert dbe.eval("IFNULL(NULL, 0)") == 0

    def test_zn(self, dbe: DbEvaluator) -> None:
        assert dbe.eval("ZN(1)") == 1
        assert dbe.eval("ZN(NULL)") == 0

    def test_isnan(self, dbe: DbEvaluator) -> None:
        if not self.supports_nan_funcs:  # FIXME: PG and CLICKHOUSe
            pytest.skip()

        assert dbe.eval("ISNAN(_MAKE_NAN())")

    def test_ifnan(self, dbe: DbEvaluator) -> None:
        if not self.supports_nan_funcs:  # FIXME: PG and CLICKHOUSe
            pytest.skip()

        assert dbe.eval("IFNAN(_MAKE_NAN(), 2)") == 2

    def test_if(self, dbe: DbEvaluator) -> None:
        assert dbe.eval("IF(TRUE, 1, 2)") == 1
        assert dbe.eval("IF(FALSE, 1, 2)") == 2

    def test_if_with_operators(self, dbe: DbEvaluator) -> None:
        assert dbe.eval("IF(1 = 1, 1, 2)") == 1
        assert dbe.eval("IF(1 != 1, 1, 2)") == 2

    def test_iif(self, dbe: DbEvaluator) -> None:
        if not self.supports_iif:  # FIXME: PG and CLICKHOUSe
            pytest.skip()
        assert dbe.eval("IIF(TRUE, 1, 2)") == 1
        assert dbe.eval("IIF(FALSE, 1, 2)") == 2

    def test_case(self, dbe: DbEvaluator) -> None:
        assert to_str(dbe.eval('CASE(1, 1, "1st", 2, "2nd", "3rd")')) == "1st"

    def test_expressions_with_conditional_flags(self, dbe: DbEvaluator, forced_literal_use: Any) -> None:
        """Check correct usage of context flags"""

        assert to_str(dbe.eval('CASE(__LIT__(1) = __LIT__(2),  TRUE, "1st", FALSE, "2nd", "3rd")')) == "2nd"
        assert to_str(dbe.eval('CASE(__LIT__(TRUE),  TRUE, "1st", FALSE, "2nd", "3rd")')) == "1st"
