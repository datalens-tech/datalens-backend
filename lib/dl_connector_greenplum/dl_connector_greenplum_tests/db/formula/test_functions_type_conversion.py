import datetime
from decimal import Decimal

import pytest
import pytz
import sqlalchemy as sa
import sqlalchemy.exc as sa_exc

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
)

from dl_connector_greenplum_tests.db.formula.base import GreenplumTestBase


GP_ROW_ORDER_SKIP_REASON = "Greenplum row order is non-deterministic without ORDER BY"


# STR


class StrTypeFunctionGreenplumTestSuite(DefaultStrTypeFunctionFormulaConnectorTestSuite):
    zero_float_to_str_value = "0.0"
    skip_custom_tz = True

    @pytest.mark.skip(reason=GP_ROW_ORDER_SKIP_REASON)
    def test_str_from_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        pass


class TestStrTypeFunctionGreenplum(GreenplumTestBase, StrTypeFunctionGreenplumTestSuite):
    pass


# FLOAT


class TestFloatTypeFunctionGreenplum(GreenplumTestBase, DefaultFloatTypeFunctionFormulaConnectorTestSuite):
    pass


# BOOL


class TestBoolTypeFunctionGreenplum(GreenplumTestBase, DefaultBoolTypeFunctionFormulaConnectorTestSuite):
    pass


# INT


class TestIntTypeFunctionGreenplum(GreenplumTestBase, DefaultIntTypeFunctionFormulaConnectorTestSuite):
    pass


# DATE


class TestDateTypeFunctionGreenplum(GreenplumTestBase, DefaultDateTypeFunctionFormulaConnectorTestSuite):
    pass


# GENERICDATETIME (& DATETIME)


class GenericDatetimeTypeFunctionGreenplumTestSuite(
    DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite,
):
    @pytest.mark.parametrize("func_name", ("GENERICDATETIME",))
    def test_genericdatetime2_gp(self, dbe: DbEvaluator, func_name: str) -> None:
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


class TestGenericDatetimeTypeFunctionGreenplum(
    GreenplumTestBase,
    GenericDatetimeTypeFunctionGreenplumTestSuite,
):
    pass


# GEOPOINT


class TestGeopointTypeFunctionGreenplum(
    GreenplumTestBase,
    DefaultGeopointTypeFunctionFormulaConnectorTestSuite,
):
    pass


# GEOPOLYGON


class TestGeopolygonTypeFunctionGreenplum(
    GreenplumTestBase,
    DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite,
):
    pass


# DB_CAST


class DbCastTypeFunctionGreenplumTestSuite(
    DefaultDbCastTypeFunctionFormulaConnectorTestSuite,
):
    def test_db_cast_greenplum(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
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

    @pytest.mark.skip(reason=GP_ROW_ORDER_SKIP_REASON)
    def test_db_cast_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        pass


class TestDbCastTypeFunctionGreenplum(
    GreenplumTestBase,
    DbCastTypeFunctionGreenplumTestSuite,
):
    pass


# TREE


class TreeTypeFunctionGreenplumTestSuite(
    DefaultTreeTypeFunctionFormulaConnectorTestSuite,
):
    @pytest.mark.skip(reason=GP_ROW_ORDER_SKIP_REASON)
    def test_tree_str(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        pass


class TestTreeTypeFunctionGreenplum(
    GreenplumTestBase,
    TreeTypeFunctionGreenplumTestSuite,
):
    pass
