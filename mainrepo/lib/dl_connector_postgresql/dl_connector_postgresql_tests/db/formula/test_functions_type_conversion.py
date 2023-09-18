import datetime
from decimal import Decimal

import pytest
import pytz
import sqlalchemy as sa
import sqlalchemy.exc as sa_exc

from dl_connector_postgresql_tests.db.formula.base import (
    PostgreSQL_9_3TestBase,
    PostgreSQL_9_4TestBase,
)
import dl_formula.core.exc as exc
from dl_formula_testing.evaluator import DbEvaluator
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
)

# STR


class StrTypeFunctionPostgreSQLTestSuite(DefaultStrTypeFunctionFormulaConnectorTestSuite):
    zero_float_to_str_value = "0.0"
    skip_custom_tz = True

    def test_str_from_array(self, dbe: DbEvaluator, data_table) -> None:
        assert to_str(dbe.eval("STR([arr_int_value])", from_=data_table)) == "{0,23,456,NULL}"
        assert to_str(dbe.eval("STR([arr_float_value])", from_=data_table)) == "{0,45,0.123,NULL}"
        assert to_str(dbe.eval("STR([arr_str_value])", from_=data_table)) == '{"","",cde,NULL}'


class TestStrTypeFunctionPostgreSQL_9_3(PostgreSQL_9_3TestBase, StrTypeFunctionPostgreSQLTestSuite):
    pass


class TestStrTypeFunctionPostgreSQL_9_4(PostgreSQL_9_4TestBase, StrTypeFunctionPostgreSQLTestSuite):
    pass


# FLOAT


class TestFloatTypeFunctionPostgreSQL_9_3(PostgreSQL_9_3TestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


class TestFloatTypeFunctionPostgreSQL_9_4(PostgreSQL_9_4TestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


# BOOL


class TestBoolTypeFunctionPostgreSQL_9_3(PostgreSQL_9_3TestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


class TestBoolTypeFunctionPostgreSQL_9_4(PostgreSQL_9_4TestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


# INT


class TestIntTypeFunctionPostgreSQL_9_3(PostgreSQL_9_3TestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


class TestIntTypeFunctionPostgreSQL_9_4(PostgreSQL_9_4TestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


# DATE


class TestDateTypeFunctionPostgreSQL_9_3(PostgreSQL_9_3TestBase, DefaultDateTypeFunctionFormulaConnectorTestSuite):
    pass


class TestDateTypeFunctionPostgreSQL_9_4(PostgreSQL_9_4TestBase, DefaultDateTypeFunctionFormulaConnectorTestSuite):
    pass


# GENERICDATETIME (& DATETIME)


class GenericDatetimeTypeFunctionPostgreSQLTestSuite(
    DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
):
    @pytest.mark.parametrize("func_name", ("GENERICDATETIME",))
    def test_genericdatetime2_pg(self, dbe: DbEvaluator, func_name: str) -> None:
        mos_tz = "Europe/Moscow"
        dt_naive = datetime.datetime(2019, 1, 3, 2, 4, 5)
        dt_naive_iso_str = dt_naive.isoformat()
        dt_as_mos = as_tz(dt_naive, tzinfo=pytz.timezone(mos_tz))
        dt_as_mos_ts = dt_as_mos.timestamp()

        dt_as_mos_to_utc = dt_as_mos.astimezone(pytz.UTC)
        dt_as_mos_to_utc_stripped = dt_strip(dt_as_mos_to_utc)

        assert dt_strip(dbe.eval(f'{func_name}({int(dt_as_mos_ts)}, "{mos_tz}")')) == dt_naive
        assert dt_strip(dbe.eval(f'{func_name}({float(dt_as_mos_ts)}, "{mos_tz}")')) == dt_naive

        # Notably, the string and genericdatetime are interpreted at the specified timezone
        # (with undefined resolution of ambiguities),
        # the number / datetime is interpreted as UTC

        # from string
        assert dt_strip(dbe.eval(f"{func_name}('{dt_naive_iso_str}', '{mos_tz}')")) == dt_as_mos_to_utc_stripped
        # from literal
        assert dt_strip(dbe.eval(f"{func_name}(#{dt_naive_iso_str}#, '{mos_tz}')")) == dt_as_mos_to_utc_stripped

        # Double-wrap (from string)
        assert dt_strip(dbe.eval(f"{func_name}({func_name}('{dt_naive_iso_str}', '{mos_tz}'), '{mos_tz}')")) == dt_naive


class TestGenericDatetimeTypeFunctionPostgreSQL_9_3(
    PostgreSQL_9_3TestBase,
    GenericDatetimeTypeFunctionPostgreSQLTestSuite,
):
    pass


class TestGenericDatetimeTypeFunctionPostgreSQL_9_4(
    PostgreSQL_9_4TestBase,
    GenericDatetimeTypeFunctionPostgreSQLTestSuite,
):
    pass


# GEOPOINT


class TestGeopointTypeFunctionPostgreSQL_9_3(
    PostgreSQL_9_3TestBase,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
):
    pass


class TestGeopointTypeFunctionPostgreSQL_9_4(
    PostgreSQL_9_4TestBase,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOLYGON


class TestGeopolygonTypeFunctionPostgreSQL_9_3(
    PostgreSQL_9_3TestBase,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
):
    pass


class TestGeopolygonTypeFunctionPostgreSQL_9_4(
    PostgreSQL_9_4TestBase,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
):
    pass


# DB_CAST


class DbCastTypeFunctionPostgreSQLTestSuite(
    DefaultDbCastTypeFunctionFormulaConnectorTestSuite,
):
    def test_db_cast_postgresql(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        value = dbe.eval("[int_value]", from_=data_table)
        assert dbe.eval('DB_CAST(FLOAT([int_value]), "double precision")', from_=data_table) == pytest.approx(
            float(value)
        )
        assert dbe.eval('DB_CAST(FLOAT([int_value]), "numeric", 5, 0)', from_=data_table) == value
        assert dbe.eval('ROUND(DB_CAST(FLOAT([int_value]), "numeric", 5, 0), 0)', from_=data_table) == value
        with pytest.raises(sa_exc.DatabaseError):
            dbe.eval('ROUND(DB_CAST(FLOAT([int_value]), "double precision"), 0)', from_=data_table)

        with pytest.raises(exc.TranslationError):
            dbe.eval('DB_CAST(FLOAT([int_value]), "double precision", 1)', from_=data_table)

        with pytest.raises(exc.TranslationError):
            dbe.eval('DB_CAST(FLOAT([int_value]), "numeric", 5)', from_=data_table)

        with pytest.raises(exc.TranslationError):
            dbe.eval('DB_CAST(FLOAT([int_value]), "numeric", "5", "3")', from_=data_table)

    def test_db_cast_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        array_int = [0, 1, -2, None]
        array_int_string = ",".join("NULL" if item is None else str(item) for item in array_int)
        assert dbe.eval(f'DB_CAST(ARRAY({array_int_string}), "smallint[]")', from_=data_table) == array_int
        assert dbe.eval(f'DB_CAST(ARRAY({array_int_string}), "integer[]")', from_=data_table) == array_int
        assert dbe.eval(f'DB_CAST(ARRAY({array_int_string}), "bigint[]")', from_=data_table) == array_int
        assert dbe.eval('DB_CAST([arr_int_value], "bigint[]")', from_=data_table) == [0, 23, 456, None]

        array_float = [0.0, 1.0, -2.0, None]
        array_float_string = ",".join("NULL" if item is None else str(item) for item in array_float)
        assert dbe.eval(f'DB_CAST(ARRAY({array_float_string}), "double precision[]")', from_=data_table) == array_float
        assert dbe.eval(f'DB_CAST(ARRAY({array_float_string}), "real[]")', from_=data_table) == array_float
        assert dbe.eval(f'DB_CAST(ARRAY({array_float_string}), "numeric[]", 5, 0)', from_=data_table) == array_float
        assert dbe.eval('DB_CAST([arr_float_value], "numeric[]", 5, 0)', from_=data_table) == [
            Decimal("0"),
            Decimal("45"),
            Decimal("0"),
            None,
        ]

        array_str = ["", "a", "NULL", None]
        array_str_string = ",".join("NULL" if item is None else f"'{item}'" for item in array_str)
        assert dbe.eval(f'DB_CAST(ARRAY({array_str_string}), "text[]")', from_=data_table) == array_str
        assert dbe.eval(f'DB_CAST(ARRAY({array_str_string}), "character varying[]")', from_=data_table) == array_str
        assert dbe.eval(f'DB_CAST(ARRAY({array_str_string}), "varchar[]")', from_=data_table) == array_str
        assert dbe.eval('DB_CAST([arr_str_value], "varchar[]")', from_=data_table) == ["", "", "cde", None]


class TestDbCastTypeFunctionPostgreSQL_9_3(
    PostgreSQL_9_3TestBase,
    DbCastTypeFunctionPostgreSQLTestSuite,
):
    pass


class TestDbCastTypeFunctionPostgreSQL_9_4(
    PostgreSQL_9_4TestBase,
    DbCastTypeFunctionPostgreSQLTestSuite,
):
    pass


# TREE


class TestTreeTypeFunctionPostgreSQL_9_3(
    PostgreSQL_9_3TestBase,
    DefaultTreeTypeFunctionFormulaConnectorTestSuite,
):
    pass


class TestTreeTypeFunctionPostgreSQL_9_4(
    PostgreSQL_9_4TestBase,
    DefaultTreeTypeFunctionFormulaConnectorTestSuite,
):
    pass
