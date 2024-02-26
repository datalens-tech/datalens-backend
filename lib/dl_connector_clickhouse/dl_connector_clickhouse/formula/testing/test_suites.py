import datetime

import clickhouse_sqlalchemy.exceptions as ch_exc
import pytest
import pytz
import sqlalchemy as sa

from dl_formula.core import nodes
from dl_formula.core.datatype import DataType
import dl_formula.core.exc as exc
from dl_formula.translation import ext_nodes
from dl_formula.translation.context import TranslationCtx
from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.base import FormulaConnectorTestBase
from dl_formula_testing.testcases.conditional_blocks import DefaultConditionalBlockFormulaConnectorTestSuite
from dl_formula_testing.testcases.functions_aggregation import DefaultMainAggFunctionFormulaConnectorTestSuite
from dl_formula_testing.testcases.functions_array import DefaultArrayFunctionFormulaConnectorTestSuite
from dl_formula_testing.testcases.functions_datetime import DefaultDateTimeFunctionFormulaConnectorTestSuite
from dl_formula_testing.testcases.functions_logical import DefaultLogicalFunctionFormulaConnectorTestSuite
from dl_formula_testing.testcases.functions_math import DefaultMathFunctionFormulaConnectorTestSuite
from dl_formula_testing.testcases.functions_string import DefaultStringFunctionFormulaConnectorTestSuite
from dl_formula_testing.testcases.functions_type_conversion import (
    DefaultDateTypeFunctionFormulaConnectorTestSuite,
    DefaultDbCastTypeFunctionFormulaConnectorTestSuite,
    DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
    DefaultStrTypeFunctionFormulaConnectorTestSuite,
)
from dl_formula_testing.testcases.literals import DefaultLiteralFormulaConnectorTestSuite
from dl_formula_testing.util import (
    as_tz,
    dt_strip,
    to_str,
    utc_ts,
)

from dl_connector_clickhouse.formula.constants import ClickHouseDialect as D


class ConditionalBlockClickHouseTestSuite(DefaultConditionalBlockFormulaConnectorTestSuite):
    def test_case_block_returning_null(self, dbe: DbEvaluator):
        # https://github.com/ClickHouse/ClickHouse/issues/7237
        assert to_str(dbe.eval('CASE 3 WHEN 1 THEN "1st" WHEN 2 THEN "2nd" END')) == ""
        assert dbe.eval("CASE 3 WHEN 1 THEN 1 WHEN 2 THEN 2 END") == 0
        assert dbe.eval("CASE 3 WHEN 1 THEN 1.1 WHEN 2 THEN 2.2 END") == 0.0
        assert dbe.eval("CASE 3 WHEN 1 THEN TRUE WHEN 2 THEN TRUE END") is False
        # NULL in THEN
        assert to_str(dbe.eval('CASE 2 WHEN 1 THEN NULL WHEN 2 THEN "2nd" ELSE "3rd" END')) == "2nd"
        assert to_str(dbe.eval('CASE 1 WHEN 1 THEN NULL WHEN 2 THEN "2nd" ELSE "3rd" END')) == ""


class MainAggFunctionClickHouseTestSuite(DefaultMainAggFunctionFormulaConnectorTestSuite):
    supports_countd_approx = True
    supports_quantile = True
    supports_median = True
    supports_arg_min_max = True
    supports_any = True
    supports_all_concat = True
    supports_top_concat = True

    def test_quantile(self, dbe: DbEvaluator, data_table: sa.Table) -> None:  # additional checks for CH
        value = dbe.eval("QUANTILE([int_value], 0.9)", from_=data_table)
        assert 80 <= value <= 90
        assert abs(value - 90) < 0.5  # allow for float approximation.

        value = dbe.eval("QUANTILE_APPROX([int_value], 0.9)", from_=data_table)
        assert 80 < value < 91  # can be either 81 or 90, apparently.


class ArrayFunctionClickHouseTestSuite(DefaultArrayFunctionFormulaConnectorTestSuite):
    def test_startswith_string_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("STARTSWITH([arr_str_value], [arr_str_value])", from_=data_table)
        assert not dbe.eval('STARTSWITH([arr_str_value], ARRAY("", "cde", NULL))', from_=data_table)


class DateTimeFunctionClickHouseTestSuite(DefaultDateTimeFunctionFormulaConnectorTestSuite):
    supports_dateadd_non_const_unit_num = True
    supports_deprecated_dateadd = True
    supports_deprecated_datepart_2 = True
    supports_datetrunc_3 = True
    supports_datetimetz = True


class LogicalFunctionClickHouseTestSuite(DefaultLogicalFunctionFormulaConnectorTestSuite):
    supports_nan_funcs = True
    supports_iif = True

    @pytest.mark.skip("Not fixed in ClickHouse yet")
    def test_bi_1052(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('IF MONTH(DATE("1989-03-17")) = 8 THEN "first" ELSE "second" END')


class MathFunctionClickHouseTestSuite(DefaultMathFunctionFormulaConnectorTestSuite):
    def test_compare(self, dbe: DbEvaluator) -> None:
        assert dbe.eval("COMPARE(0, 0, 0)") == 0
        assert dbe.eval("COMPARE(0.123, 0.123, 0)") == 0
        assert dbe.eval("COMPARE(0.123, 0.123, 0.1)") == 0
        assert dbe.eval("COMPARE(0.123, 0.12, 0.1)") == 0
        assert dbe.eval("COMPARE(0.123, 0.12, 0.001)") == 1
        assert dbe.eval("COMPARE(0.12, 0.123, 0.001)") == -1


class StringFunctionClickHouseTestSuite(DefaultStringFunctionFormulaConnectorTestSuite):
    def test_utf8(self, dbe: DbEvaluator) -> None:
        # UTF8() is only available for CLICKHOUSE
        binary_str = TranslationCtx(expression=sa.text(r"'\xcf\xf0\xe8\xe2\xe5\xf2'"), data_type=DataType.STRING)
        assert (
            dbe.eval(
                nodes.Formula.make(
                    nodes.FuncCall.make(
                        name="UTF8",
                        args=[ext_nodes.CompiledExpression.make(binary_str), nodes.LiteralString.make("CP-1251")],
                    )
                )
            )
            == "Привет"
        )


class StrTypeFunctionClickHouseTestSuite(DefaultStrTypeFunctionFormulaConnectorTestSuite):
    bool_of_null_is_false = True

    def test_str_from_array(self, dbe: DbEvaluator, data_table) -> None:
        assert to_str(dbe.eval("STR([arr_int_value])", from_=data_table)) == "[0,23,456,NULL]"
        assert to_str(dbe.eval("STR([arr_float_value])", from_=data_table)) == "[0,45,0.123,NULL]"
        assert to_str(dbe.eval("STR([arr_str_value])", from_=data_table)) == "['','','cde',NULL]"


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


class DateTypeFunctionClickHouseTestSuite(DefaultDateTypeFunctionFormulaConnectorTestSuite):
    def test_date2(self, dbe: DbEvaluator) -> None:
        ts = utc_ts(2019, 1, 2, 23, 4, 5)
        tz = "Europe/Moscow"
        expected = datetime.date(2019, 1, 3)
        assert dbe.eval('DATE({}, "{}")'.format(int(ts), tz)) == expected
        assert dbe.eval('DATE({}, "{}")'.format(float(ts), tz)) == expected


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


class LiteralFunctionClickHouseTestSuite(DefaultLiteralFormulaConnectorTestSuite):
    supports_microseconds = False
    supports_utc = True
    supports_custom_tz = True
    default_tz = None
