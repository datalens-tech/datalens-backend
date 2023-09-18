import datetime

import clickhouse_sqlalchemy.exceptions as ch_exc
import pytest
import pytz
import sqlalchemy as sa

from dl_connector_clickhouse.formula.constants import ClickHouseDialect as D
from dl_connector_clickhouse_tests.db.formula.base import ClickHouse_21_8TestBase
import dl_formula.core.exc as exc
from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.base import FormulaConnectorTestBase
from dl_formula_testing.testcases.functions_type_conversion import (
    DefaultBoolTypeFunctionFormulaConnectorTestSuite,
    DefaultDateTypeFunctionFormulaConnectorTestSuite,
    DefaultDbCastTypeFunctionFormulaConnectorTestSuite,
    DefaultFloatTypeFunctionFormulaConnectorTestSuite,
    DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
    DefaultIntTypeFunctionFormulaConnectorTestSuite,
    DefaultStrTypeFunctionFormulaConnectorTestSuite,
    DefaultTreeTypeFunctionFormulaConnectorTestSuite,
)
from dl_formula_testing.util import (
    as_tz,
    dt_strip,
    to_str,
    utc_ts,
)

# STR


class StrTypeFunctionClickHouseTestSuite(DefaultStrTypeFunctionFormulaConnectorTestSuite):
    bool_of_null_is_false = True

    def test_str_from_array(self, dbe: DbEvaluator, data_table) -> None:
        assert to_str(dbe.eval("STR([arr_int_value])", from_=data_table)) == "[0,23,456,NULL]"
        assert to_str(dbe.eval("STR([arr_float_value])", from_=data_table)) == "[0,45,0.123,NULL]"
        assert to_str(dbe.eval("STR([arr_str_value])", from_=data_table)) == "['','','cde',NULL]"


class TestStrTypeFunctionClickHouse_21_8(ClickHouse_21_8TestBase, StrTypeFunctionClickHouseTestSuite):
    pass


# FLOAT


class TestFloatTypeFunctionClickHouse_21_8(ClickHouse_21_8TestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


# BOOL


class TestBoolTypeFunctionClickHouse_21_8(ClickHouse_21_8TestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


# INT


class TestIntTypeFunctionClickHouse_21_8(ClickHouse_21_8TestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


# DATE


class DateTypeFunctionClickHouseTestSuite(DefaultDateTypeFunctionFormulaConnectorTestSuite):
    def test_date2(self, dbe: DbEvaluator) -> None:
        ts = utc_ts(2019, 1, 2, 23, 4, 5)
        tz = "Europe/Moscow"
        expected = datetime.date(2019, 1, 3)
        assert dbe.eval('DATE({}, "{}")'.format(int(ts), tz)) == expected
        assert dbe.eval('DATE({}, "{}")'.format(float(ts), tz)) == expected


class TestDateTypeFunctionClickHouse_21_8(ClickHouse_21_8TestBase, DateTypeFunctionClickHouseTestSuite):
    pass


# DATE_PARSE


class TestDateParseTypeFunctionClickHouse_21_8(ClickHouse_21_8TestBase, FormulaConnectorTestBase):
    pass


# GENERICDATETIME (& DATETIME)


class GenericDatetimeTypeFunctionClickHouseTestSuite(DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite):
    @pytest.mark.parametrize("func_name", ("GENERICDATETIME", "DATETIME"))
    def test_genericdatetime2_ch(self, dbe: DbEvaluator, func_name: str) -> None:
        mos_tz = "Europe/Moscow"
        dt_naive = datetime.datetime(2019, 1, 3, 2, 4, 5)
        dt_naive_iso_str = dt_naive.isoformat()
        dt_as_mos = as_tz(dt_naive, tzinfo=pytz.timezone(mos_tz))
        dt_as_mos_ts = dt_as_mos.timestamp()

        dt_as_dbtz = as_tz(dt_naive, tzinfo=dbe.db.tzinfo)
        dt_as_dbtz_to_mos = dt_as_dbtz.astimezone(pytz.timezone(mos_tz))
        dt_as_dbtz_to_mos_stripped = dt_strip(dt_as_dbtz_to_mos)

        assert dt_strip(dbe.eval(f'{func_name}({int(dt_as_mos_ts)}, "{mos_tz}")')) == dt_naive
        assert dt_strip(dbe.eval(f'{func_name}({float(dt_as_mos_ts)}, "{mos_tz}")')) == dt_naive

        # Notably, the string and genericdatetime are interpreted at the specified timezone
        # (with undefined resolution of ambiguities),
        # the number / datetime is interpreted as UTC

        # from string
        assert dt_strip(dbe.eval(f"{func_name}('{dt_naive_iso_str}', '{mos_tz}')")) == dt_naive
        # from literal
        assert dt_strip(dbe.eval(f"{func_name}(#{dt_naive_iso_str}#, '{mos_tz}')")) == dt_as_dbtz_to_mos_stripped

        # Double-wrap (from string)
        assert dt_strip(dbe.eval(f"{func_name}({func_name}('{dt_naive_iso_str}', '{mos_tz}'), '{mos_tz}')")) == dt_naive


class TestGenericDatetimeTypeFunctionClickHouse_21_8(
    ClickHouse_21_8TestBase,
    GenericDatetimeTypeFunctionClickHouseTestSuite,
):
    pass


# GENERICDATETIME_PARSE (& DATETIME_PARSE)


class GenericDatetimeParseTypeFunctionClickHouseTestSuite(FormulaConnectorTestBase):
    @pytest.mark.parametrize("func_name", ("GENERICDATETIME_PARSE", "DATETIME_PARSE"))
    def test_new_datetime_parse(self, dbe: DbEvaluator, func_name: str) -> None:
        if not dbe.dialect & D.and_above(D.CLICKHOUSE_21_8):
            pytest.skip()

        assert dt_strip(dbe.eval(f'{func_name}("2019-01-02 03:04:05")')) == (datetime.datetime(2019, 1, 2, 3, 4, 5))
        assert dt_strip(dbe.eval(f'{func_name}("2019-01-02 03:04:05+02")')) == (
            datetime.datetime(2019, 1, 1, 20, 4, 5)
        )  # moved to NYC timezone
        assert dt_strip(dbe.eval(f'{func_name}("20190102030405")')) == (datetime.datetime(2019, 1, 2, 3, 4, 5))
        assert dt_strip(dbe.eval(f'{func_name}("20190102 030405")')) == (datetime.datetime(2019, 1, 2, 3, 4, 5))
        assert dt_strip(dbe.eval(f'{func_name}("2019.01.02 03:04:05")')) == (datetime.datetime(2019, 1, 2, 3, 4, 5))
        assert dt_strip(dbe.eval(f'{func_name}("02/01/2019 03:04:05")')) == (datetime.datetime(2019, 1, 2, 3, 4, 5))
        assert dt_strip(dbe.eval(f'{func_name}("2019-01-02 03:04")')) == (datetime.datetime(2019, 1, 2, 3, 4, 0))
        assert dt_strip(dbe.eval(f'{func_name}("2019-01-02 030405")')) == (datetime.datetime(2019, 1, 2, 3, 4, 5))
        assert dt_strip(dbe.eval(f'{func_name}("2019 Jan 02 03:04:05")')) == (datetime.datetime(2019, 1, 2, 3, 4, 5))


class TestGenericDatetimeParseTypeFunctionClickHouse_21_8(
    ClickHouse_21_8TestBase,
    GenericDatetimeParseTypeFunctionClickHouseTestSuite,
):
    pass


# GEOPOINT


class TestGeopointTypeFunctionClickHouse_21_8(
    ClickHouse_21_8TestBase,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOLYGON


class TestGeopolygonTypeFunctionClickHouse_21_8(
    ClickHouse_21_8TestBase,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
):
    pass


# DB_CAST


class DbCastTypeFunctionClickHouseTestSuite(DefaultDbCastTypeFunctionFormulaConnectorTestSuite):
    def test_db_cast_clickhouse(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        value = dbe.eval("[int_value]", from_=data_table)
        assert dbe.eval('DB_CAST(FLOAT([int_value]), "Float32")', from_=data_table) == pytest.approx(float(value))
        assert dbe.eval('DB_CAST(FLOAT([int_value]), "Float64")', from_=data_table) == pytest.approx(float(value))
        assert dbe.eval('DB_CAST(FLOAT([int_value]), "Decimal", 5, 0)', from_=data_table) == value

        assert dbe.eval('DB_CAST(FLOAT([int_value]), "Float64") / 0', from_=data_table)  # Doesn't raise an error
        with pytest.raises(ch_exc.DatabaseException):
            dbe.eval('DB_CAST(FLOAT([int_value]), "Decimal", 5, 0) / 0', from_=data_table)  # Not supported by CH

        with pytest.raises(exc.TranslationError):
            dbe.eval('DB_CAST(FLOAT([int_value]), "Decimal", 1)', from_=data_table)


class TestDbCastTypeFunctionClickHouse_21_8(
    ClickHouse_21_8TestBase,
    DbCastTypeFunctionClickHouseTestSuite,
):
    pass


# TREE


class TestTreeTypeFunctionClickHouse_21_8(
    ClickHouse_21_8TestBase,
    DefaultTreeTypeFunctionFormulaConnectorTestSuite,
):
    pass
