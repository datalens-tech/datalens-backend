from __future__ import annotations

import datetime
from typing import (
    ClassVar,
    Optional,
    Union,
)

import dateutil
import pytest
import pytz

from dl_formula.definitions.literals import literal
from dl_formula.shortcuts import n
from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.base import FormulaConnectorTestBase
from dl_formula_testing.util import (
    dt_strip,
    to_str,
)


def as_dt(value: Union[str, datetime.datetime]) -> datetime.datetime:
    if isinstance(value, str):
        value = dateutil.parser.parse(value)
    assert isinstance(value, datetime.datetime)
    return value


class DefaultLiteralFormulaConnectorTestSuite(FormulaConnectorTestBase):
    supports_microseconds: ClassVar[bool]
    supports_utc: ClassVar[bool]
    supports_custom_tz: ClassVar[bool]
    default_tz: ClassVar[Optional[datetime.tzinfo]] = None
    recognizes_datetime_type: ClassVar[bool] = True

    def test_number(self, dbe: DbEvaluator) -> None:
        assert dbe.eval("1") == 1
        assert type(dbe.eval("1")) is int
        assert dbe.eval("1.2") == 1.2

    def test_string(self, dbe: DbEvaluator) -> None:
        assert to_str(dbe.eval('"qwerty"')) == "qwerty"
        assert to_str(dbe.eval('"Lorem\\nipsum"')) == "Lorem\nipsum"

    def test_null(self, dbe: DbEvaluator) -> None:
        assert dbe.eval("NULL") is None

    def test_bool(self, dbe: DbEvaluator) -> None:
        assert dbe.eval("TRUE") is True
        assert type(dbe.eval("TRUE")) is bool
        assert dbe.eval("FALSE") is False
        assert type(dbe.eval("FALSE")) is bool

    def test_date(self, dbe: DbEvaluator) -> None:
        assert dbe.eval("#2019-01-02#") == datetime.date(2019, 1, 2)
        assert type(dbe.eval("#2019-01-02#")) is datetime.date

    def test_datetime(self, dbe: DbEvaluator) -> None:
        assert dt_strip(dbe.eval("#2019-01-02 12:34:56#")) == datetime.datetime(2019, 1, 2, 12, 34, 56)
        if self.recognizes_datetime_type:
            assert type(dbe.eval("#2019-01-02 12:34:56#")) is datetime.datetime

    def test_datetime_extended(self, dbe: DbEvaluator) -> None:
        # Without microseconds
        assert as_dt(
            dbe.db.scalar(literal(datetime.datetime(2020, 4, 8, 12, 34, 56), d=dbe.dialect))
        ) == datetime.datetime(2020, 4, 8, 12, 34, 56, tzinfo=self.default_tz)

        # With microseconds
        assert as_dt(
            dbe.db.scalar(literal(datetime.datetime(2020, 4, 8, 12, 34, 56, microsecond=7890), d=dbe.dialect))
        ) == datetime.datetime(
            2020, 4, 8, 12, 34, 56, microsecond=7890 if self.supports_microseconds else 0, tzinfo=self.default_tz
        )

        # With UTC without microseconds
        if self.supports_utc:
            assert as_dt(
                dbe.db.scalar(
                    literal(datetime.datetime(2020, 4, 8, 12, 34, 56, tzinfo=datetime.timezone.utc), d=dbe.dialect)
                )
            ) == datetime.datetime(2020, 4, 8, 12, 34, 56, tzinfo=datetime.timezone.utc)

        # With UTC and microseconds
        if self.supports_utc:
            assert as_dt(
                dbe.db.scalar(
                    literal(
                        datetime.datetime(2020, 4, 8, 12, 34, 56, microsecond=7890, tzinfo=datetime.timezone.utc),
                        d=dbe.dialect,
                    )
                )
            ) == datetime.datetime(
                2020,
                4,
                8,
                12,
                34,
                56,
                microsecond=7890 if self.supports_microseconds else 0,
                tzinfo=datetime.timezone.utc,
            )

        # With custom tz
        if self.supports_custom_tz:
            assert as_dt(
                dbe.db.scalar(
                    literal(
                        pytz.timezone("Europe/Moscow").localize(datetime.datetime(2020, 4, 8, 15, 34, 56, tzinfo=None)),
                        d=dbe.dialect,
                    )
                )
            ) == datetime.datetime(2020, 4, 8, 12, 34, 56, tzinfo=datetime.timezone.utc)

    def test_arrays(self, dbe) -> None:
        if not self.supports_arrays:
            pytest.skip("Not supported")
        assert dbe.eval(n.formula(n.func.GET_ITEM(n.lit([1, 4, 6]), 2))) == 4
        assert dbe.eval(n.formula(n.func.GET_ITEM(n.lit([1.1, 4.2, 6.3]), 2))) == 4.2
        assert dbe.eval(n.formula(n.func.GET_ITEM(n.lit(["qwe", "rty", "uio"]), 2))) == "rty"
