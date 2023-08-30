from __future__ import annotations

import datetime
import math
from typing import ClassVar

import pytest

from bi_formula.connectors.base.testing.base import FormulaConnectorTestBase
from bi_formula.testing.evaluator import DbEvaluator
from bi_formula.testing.util import to_str, dt_strip


class DefaultMathFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    supports_atan_2_in_origin: ClassVar[bool] = True
    supports_float_div: ClassVar[bool] = True

    def test_trigonometric_functions(self, dbe: DbEvaluator):
        assert dbe.eval('ACOS(1)') == 0
        assert dbe.eval('ACOS(0)') == pytest.approx(math.pi / 2)

        assert dbe.eval('ASIN(0)') == 0
        assert dbe.eval('ASIN(-1)') == pytest.approx(-math.pi / 2)

        assert dbe.eval('ATAN(0)') == 0
        assert dbe.eval('ATAN(-1)') == pytest.approx(-math.pi / 4)

        if self.supports_atan_2_in_origin:
            assert dbe.eval('ATAN2(0, 0)') == 0
        assert dbe.eval('ATAN2(1, 1)') == pytest.approx(math.pi / 4)
        assert dbe.eval('ATAN2(-1, -1)') == pytest.approx(-3 * math.pi / 4)
        assert dbe.eval('ATAN2(1, -1)') == pytest.approx(3 * math.pi / 4)
        assert dbe.eval('ATAN2(-1, 1)') == pytest.approx(-math.pi / 4)
        assert dbe.eval('ATAN2(1, 0)') == pytest.approx(math.pi / 2)
        assert dbe.eval('ATAN2(-1, 0)') == pytest.approx(-math.pi / 2)

        assert dbe.eval('COS(0)') == pytest.approx(1)
        assert dbe.eval('COS(PI()/2)') == pytest.approx(0)

        assert dbe.eval('COT(PI()/4)') == pytest.approx(1)
        assert dbe.eval('COT(PI()/2)') == pytest.approx(0)

        assert dbe.eval('DEGREES(0)') == 0
        assert dbe.eval('DEGREES(PI())') == pytest.approx(180)

        # MySQL uses double precision for PI internally, but displays only 7 decimal places
        assert dbe.eval('PI()') == pytest.approx(math.pi, abs=1e-06)

        assert dbe.eval('RADIANS(0)') == 0
        assert dbe.eval('RADIANS(180)') == pytest.approx(math.pi)

        assert dbe.eval('SIN(0)') == pytest.approx(0)
        assert dbe.eval('SIN(PI()/2)') == pytest.approx(1)

        assert dbe.eval('TAN(0)') == 0
        assert dbe.eval('TAN(PI()/4)') == pytest.approx(1)

    def test_exponential_functions(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('EXP(0)') == pytest.approx(1)
        assert dbe.eval('EXP(1)') == pytest.approx(math.e)

        assert dbe.eval('LN(1)') == pytest.approx(0)
        assert dbe.eval('LN(EXP(2))') == pytest.approx(2)

        assert dbe.eval('LOG10(1)') == pytest.approx(0)
        assert dbe.eval('LOG10(1000)') == pytest.approx(3)

        assert dbe.eval('LOG(1, 2.6)') == pytest.approx(0)
        assert dbe.eval('LOG(1024, 2)') == pytest.approx(10)

        assert dbe.eval('POWER(2.3, 4.5)') == pytest.approx(2.3**4.5)

        assert dbe.eval('SQRT(9)') == 3

        assert dbe.eval('SQUARE(9)') == 81

    def test_rounding_functions(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('CEILING(2.7)') == 3
        assert dbe.eval('CEILING(-2.7)') == -2

        assert dbe.eval('FLOOR(2.7)') == 2
        assert dbe.eval('FLOOR(-2.7)') == -3

        assert dbe.eval('ROUND(2.3)') == 2
        assert float(dbe.eval('ROUND(2.1234, 2)')) == 2.12

    def test_least(self, dbe: DbEvaluator) -> None:
        # single arg
        assert dbe.eval('LEAST(3.4)') == pytest.approx(3.4)
        # multiple args
        assert dbe.eval('LEAST(3.4, 2.6)') == pytest.approx(2.6)
        assert to_str(dbe.eval('LEAST("3.4", "2.6")')) == '2.6'
        assert dbe.eval('LEAST(#2019-01-02#, #2019-01-17#)') == datetime.date(2019, 1, 2)
        assert dt_strip(dbe.eval(
            'LEAST(#2019-01-02 04:03:02#, #2019-01-17 03:02:01#)')
        ) == datetime.datetime(2019, 1, 2, 4, 3, 2)
        assert dbe.eval('LEAST(TRUE, FALSE)') is False
        assert dbe.eval('LEAST(34, 5, 7, 3, 99, 1, 2, 2, 56)') == 1
        assert dbe.eval('LEAST(5.6, 1.2, 7.8, 3.4)') == pytest.approx(1.2)
        assert dbe.eval('LEAST(#2019-01-02#, #2019-01-17#, #2019-01-10#)') == datetime.date(2019, 1, 2)

    def test_greatest(self, dbe: DbEvaluator) -> None:
        # single arg
        assert dbe.eval('GREATEST(3.4)') == pytest.approx(3.4)
        # multiple args
        assert dbe.eval('GREATEST(3.4, 2.6)') == pytest.approx(3.4)
        assert to_str(dbe.eval('GREATEST("3.4", "2.6")')) == '3.4'
        assert dbe.eval('GREATEST(#2019-01-02#, #2019-01-17#)') == datetime.date(2019, 1, 17)
        assert dt_strip(dbe.eval(
            'GREATEST(#2019-01-02 04:03:02#, #2019-01-17 03:02:01#)'
        )) == datetime.datetime(2019, 1, 17, 3, 2, 1)
        assert dbe.eval('GREATEST(TRUE, FALSE)') is True
        assert dbe.eval('GREATEST(34, 5, 7, 3, 99, 1, 2, 2, 56)') == 99
        assert dbe.eval('GREATEST(5.6, 1.2, 7.8, 3.4)') == pytest.approx(7.8)
        assert dbe.eval('GREATEST(#2019-01-02#, #2019-01-17#, #2019-01-10#)') == datetime.date(2019, 1, 17)

    def test_arithmetic_functions(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('DIV(4, 2)') == 2
        assert dbe.eval('DIV(5, 3)') == 1
        assert dbe.eval('DIV_SAFE(5, 2)') == 2
        assert dbe.eval('DIV_SAFE(5, 0)') is None
        assert dbe.eval('DIV_SAFE(5, 0, 42)') == 42
        if self.supports_float_div:
            assert dbe.eval('DIV(5.0, 2)') == 2
            assert dbe.eval('DIV(5, 2.0)') == 2
            assert dbe.eval('DIV_SAFE(5.0, 2)') == 2
            assert dbe.eval('DIV_SAFE(5.0, 0)') is None
            assert dbe.eval('DIV_SAFE(5.0, 0, 42)') == 42
            assert dbe.eval('FDIV_SAFE(5.0, 2)') == 2.5
            assert dbe.eval('FDIV_SAFE(1, 0.0)') is None
            assert dbe.eval('FDIV_SAFE(1, 0.0, 42)') == 42

    def test_abs_sign(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('ABS(-3)') == 3

        assert dbe.eval('SIGN(-7)') == -1
        assert dbe.eval('SIGN(0)') == 0
        assert dbe.eval('SIGN(7)') == 1
