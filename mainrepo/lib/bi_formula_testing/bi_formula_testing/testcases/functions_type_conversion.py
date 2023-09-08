from __future__ import annotations

import datetime
import re
from typing import ClassVar

import pytest
import pytz
import sqlalchemy as sa

from bi_formula_testing.util import to_str, utc_ts, dt_strip, to_datetime, as_tz
from bi_formula_testing.testcases.base import FormulaConnectorTestBase
from bi_formula_testing.evaluator import DbEvaluator


class DefaultStrTypeFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    zero_float_to_str_value: ClassVar[str] = '0'
    bool_of_null_is_false: bool = False
    skip_custom_tz: ClassVar[bool] = False
    skip_max_ints: ClassVar[bool] = False

    def test_str_numbers(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('STR(0)') == '0'
        assert to_str(dbe.eval('STR(0.0)')) == self.zero_float_to_str_value
        assert to_str(dbe.eval('STR(48.2)')) == '48.2'
        assert to_str(dbe.eval('STR(-48.2)')) == '-48.2'
        assert to_str(dbe.eval('STR(4887.2752789)')) == '4887.2752789'
        assert to_str(dbe.eval('STR(-4887.2752789)')) == '-4887.2752789'
        assert to_str(dbe.eval('STR(8010)')) == '8010'
        assert to_str(dbe.eval('STR(-8010)')) == '-8010'

    def test_str_from_bool(self, dbe: DbEvaluator) -> None:
        assert to_str(dbe.eval('STR(TRUE)')) == 'True'
        assert to_str(dbe.eval('STR(FALSE)')) == 'False'

    def test_str_from_str(self, dbe: DbEvaluator) -> None:
        assert to_str(dbe.eval('STR("qwe")')) == 'qwe'

    def test_str_from_date(self, dbe: DbEvaluator) -> None:
        assert to_str(dbe.eval('STR(#2019-01-02#)')) == '2019-01-02'

    def test_str_from_datetime(self, dbe: DbEvaluator) -> None:
        assert re.match(r'2019-01-02 03:04:05(\.\d+)?', to_str(dbe.eval('STR(#2019-01-02 03:04:05#)')))

    def test_str_from_datetime_custom_tz(self, dbe: DbEvaluator) -> None:
        if self.skip_custom_tz:
            pytest.skip('Custom TZ is not supported in function STR for this dialect')

        assert to_str(dbe.eval('STR(DATETIME("2019-01-02 03:04:05", "Pacific/Chatham"))')) == '2019-01-02 03:04:05'
        assert to_str(
            dbe.eval('STR(GENERICDATETIME("2019-01-02 03:04:05", "Pacific/Chatham"))')) == '2019-01-02 03:04:05'

    def test_str_from_max_ints(self, dbe: DbEvaluator) -> None:
        if self.skip_custom_tz:
            pytest.skip('Max int test is not supported in function STR for this dialect')

        # FIXME: Except for MSSQLSRV
        max_signed_bigint = 2 ** 63 - 1
        min_signed_bigint = - 2 ** 63
        assert to_str(dbe.eval(f'STR({max_signed_bigint})')) == str(max_signed_bigint)
        assert to_str(dbe.eval(f'STR({min_signed_bigint})')) == str(min_signed_bigint)

    def test_str_from_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.supports_arrays:
            pytest.skip('Arrays are not supported')

        raise NotImplementedError('This test is dialect-dependent')

    def test_str_from_const_null(self, dbe: DbEvaluator):
        if self.empty_str_is_null:
            pytest.skip()

        assert dbe.eval('STR(NULL)') is None

        if self.bool_of_null_is_false:
            # TODO: in any fresh CH, apparently, `if(NULL, 1, 0) == 0`.
            # Might want to reconsider the used formulas.
            assert dbe.eval('STR(BOOL(NULL))') == 'False'
        else:
            assert dbe.eval('STR(BOOL(NULL))') is None

        assert dbe.eval('STR(INT(NULL))') is None
        assert dbe.eval('STR(FLOAT(NULL))') is None
        assert dbe.eval('STR(DATE(NULL))') is None
        assert dbe.eval('STR(DATETIME(NULL))') is None
        assert dbe.eval('STR(GENERICDATETIME(NULL))') is None
        assert dbe.eval('STR(GEOPOINT(NULL))') is None
        assert dbe.eval('STR(GEOPOLYGON(NULL))') is None

    def test_str_from_data_null(self, dbe: DbEvaluator, null_data_table: sa.Table):
        if self.empty_str_is_null:
            pytest.skip()

        if self.bool_of_null_is_false:
            # See `test_str_from_const_null` note above.
            assert dbe.eval('STR([bool_null])', from_=null_data_table) == 'False'
        else:
            assert dbe.eval('STR([bool_null])', from_=null_data_table) is None

        assert dbe.eval('STR([int_null])', from_=null_data_table) is None
        assert dbe.eval('STR([float_null])', from_=null_data_table) is None
        assert dbe.eval('STR([date_null])', from_=null_data_table) is None
        assert dbe.eval('STR([datetime_null])', from_=null_data_table) is None
        assert dbe.eval('STR([geopoint_null])', from_=null_data_table) is None
        assert dbe.eval('STR([geopolygon_null])', from_=null_data_table) is None

        if self.supports_uuid:
            assert dbe.eval('STR([uuid_null])', from_=null_data_table) is None


class DefaultFloatTypeFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    def test_float(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('FLOAT("1.2")') == 1.2
        assert dbe.eval('FLOAT(8)') == 8.0
        assert type(dbe.eval('FLOAT(8)')) == float
        assert dbe.eval('FLOAT(TRUE)') == 1.0
        assert dbe.eval('FLOAT(FALSE)') == 0.0
        assert dbe.eval('FLOAT(#2019-01-02#)') == utc_ts(2019, 1, 2)
        assert type(dbe.eval('FLOAT(#2019-01-02#)')) == float
        assert dbe.eval('FLOAT(#2019-01-02 03:04:05#)') == (
            as_tz(datetime.datetime(2019, 1, 2, 3, 4, 5), tzinfo=dbe.db.tzinfo).timestamp()
        )
        assert type(dbe.eval('FLOAT(#2019-01-02 03:04:05#)')) == float

    def test_float_from_null(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('FLOAT(NULL)') is None

        if self.null_casts_to_false:
            assert dbe.eval('FLOAT(BOOL(NULL))') == 0.0
        else:
            assert dbe.eval('FLOAT(BOOL(NULL))') is None

        if not self.empty_str_is_null:
            assert dbe.eval('FLOAT(STR(NULL))') is None

        assert dbe.eval('FLOAT(INT(NULL))') is None
        assert dbe.eval('FLOAT(FLOAT(NULL))') is None
        assert dbe.eval('FLOAT(DATE(NULL))') is None
        assert dbe.eval('FLOAT(DATETIME(NULL))') is None
        assert dbe.eval('FLOAT(GENERICDATETIME(NULL))') is None

    def test_float_from_data_null(self, dbe: DbEvaluator, null_data_table: sa.Table) -> None:
        if self.null_casts_to_false:
            assert dbe.eval('FLOAT([bool_null])', from_=null_data_table) == 0.0
        else:
            assert dbe.eval('FLOAT([bool_null])', from_=null_data_table) is None

        if not self.empty_str_is_null:
            assert dbe.eval('FLOAT([str_null])', from_=null_data_table) is None

        assert dbe.eval('FLOAT([int_null])', from_=null_data_table) is None
        assert dbe.eval('FLOAT([float_null])', from_=null_data_table) is None
        assert dbe.eval('FLOAT([date_null])', from_=null_data_table) is None
        assert dbe.eval('FLOAT([datetime_null])', from_=null_data_table) is None


class DefaultBoolTypeFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    def test_bool(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('BOOL(1.2)') is True
        assert dbe.eval('BOOL(0.0)') is False
        assert dbe.eval('BOOL(8)') is True
        assert dbe.eval('BOOL(0)') is False
        assert dbe.eval('BOOL(TRUE)') is True
        assert dbe.eval('BOOL(FALSE)') is False
        assert dbe.eval('BOOL("qwe")') is True
        assert dbe.eval('BOOL("")') is False
        assert type(dbe.eval('BOOL(1)')) == bool
        assert dbe.eval('BOOL(#2019-01-02#)') is True
        assert dbe.eval('BOOL(#2019-01-02 03:04:05#)') is True

    def test_bool_from_null(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('BOOL(NULL)') is None
        assert dbe.eval('BOOL(BOOL(NULL))') is None
        if not self.bool_is_expression:
            assert dbe.eval('BOOL(STR(NULL))') is None
        if not self.null_casts_to_number:
            assert dbe.eval('BOOL(INT(NULL))') is None
            assert dbe.eval('BOOL(FLOAT(NULL))') is None
        assert dbe.eval('BOOL(DATE(NULL))') is None
        assert dbe.eval('BOOL(DATETIME(NULL))') is None
        assert dbe.eval('BOOL(GENERICDATETIME(NULL))') is None

    def test_bool_from_data_null(self, dbe: DbEvaluator, null_data_table: sa.Table):
        assert dbe.eval('BOOL([bool_null])', from_=null_data_table) is None
        if not self.bool_is_expression:
            assert dbe.eval('BOOL([str_null])', from_=null_data_table) is None
        if not self.null_casts_to_number:
            assert dbe.eval('BOOL([int_null])', from_=null_data_table) is None
            assert dbe.eval('BOOL([float_null])', from_=null_data_table) is None
        assert dbe.eval('BOOL([date_null])', from_=null_data_table) is None
        assert dbe.eval('BOOL([datetime_null])', from_=null_data_table) is None


class DefaultIntTypeFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    def test_int(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('INT(432.65)') == 432
        assert dbe.eval('INT(432.35)') == 432
        assert dbe.eval('INT(432)') == 432
        assert dbe.eval('INT(TRUE)') == 1
        assert dbe.eval('INT(FALSE)') == 0
        assert dbe.eval('INT("83")') == 83
        assert dbe.eval('INT(#2019-01-02#)') == int(utc_ts(2019, 1, 2))
        assert type(dbe.eval('INT(#2019-01-02#)')) == int
        assert dbe.eval('INT(#2019-01-02 03:04:05#)') == (
            int(as_tz(datetime.datetime(2019, 1, 2, 3, 4, 5), tzinfo=dbe.db.tzinfo).timestamp())
        )
        assert type(dbe.eval('INT(#2019-01-02 03:04:05#)')) == int

    def test_int_from_null(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('INT(NULL)') is None

        if self.null_casts_to_false:
            assert dbe.eval('INT(BOOL(NULL))') == 0
        else:
            assert dbe.eval('INT(BOOL(NULL))') is None

        if not self.empty_str_is_null:  # NULL can be interpreted as ''
            assert dbe.eval('INT(STR(NULL))') is None
        assert dbe.eval('INT(INT(NULL))') is None
        assert dbe.eval('INT(FLOAT(NULL))') is None
        assert dbe.eval('INT(DATE(NULL))') is None
        assert dbe.eval('INT(DATETIME(NULL))') is None
        assert dbe.eval('INT(GENERICDATETIME(NULL))') is None

    def test_int_from_data_null(self, dbe: DbEvaluator, null_data_table: sa.Table) -> None:
        if self.null_casts_to_false:
            assert dbe.eval('INT([bool_null])', from_=null_data_table) == 0
        else:
            assert dbe.eval('INT([bool_null])', from_=null_data_table) is None

        if not self.empty_str_is_null:  # NULL can be interpreted as ''
            assert dbe.eval('INT([str_null])', from_=null_data_table) is None
        assert dbe.eval('INT([int_null])', from_=null_data_table) is None
        assert dbe.eval('INT([float_null])', from_=null_data_table) is None
        assert dbe.eval('INT([date_null])', from_=null_data_table) is None
        assert dbe.eval('INT([datetime_null])', from_=null_data_table) is None


class DefaultDateTypeFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    def test_date(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('DATE(#2019-01-02#)') == datetime.date(2019, 1, 2)
        assert dbe.eval('DATE(#2019-01-02 03:04:05#)') == datetime.date(2019, 1, 2)
        assert dbe.eval('DATE("2019-01-02")') == datetime.date(2019, 1, 2)
        expected_date = datetime.datetime(
            2019, 1, 2, 3, 4, 5, tzinfo=pytz.UTC
        ).astimezone(dbe.db.tzinfo).date()  # Note that the result depends on db's TZ
        assert dbe.eval('DATE({})'.format(int(utc_ts(2019, 1, 2, 3, 4, 5)))) == expected_date
        assert dbe.eval('DATE({})'.format(float(utc_ts(2019, 1, 2, 3, 4, 5)))) == expected_date

    def test_date_from_null(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('DATE(NULL)') is None
        if not self.empty_str_is_null:
            assert dbe.eval('DATE(STR(NULL))') is None
        assert dbe.eval('DATE(INT(NULL))') is None
        assert dbe.eval('DATE(FLOAT(NULL))') is None
        assert dbe.eval('DATE(DATE(NULL))') is None
        assert dbe.eval('DATE(DATETIME(NULL))') is None
        assert dbe.eval('DATE(GENERICDATETIME(NULL))') is None

    def test_date_from_data_null(self, dbe: DbEvaluator, null_data_table: sa.Table) -> None:
        if not self.empty_str_is_null:  # NULL can be interpreted as ''
            assert dbe.eval('DATE([str_null])', from_=null_data_table) is None
        assert dbe.eval('DATE([int_null])', from_=null_data_table) is None
        assert dbe.eval('DATE([float_null])', from_=null_data_table) is None
        assert dbe.eval('DATE([date_null])', from_=null_data_table) is None
        assert dbe.eval('DATE([datetime_null])', from_=null_data_table) is None


class DefaultGenericDatetimeTypeFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    @pytest.mark.parametrize('func_name', ('GENERICDATETIME', 'DATETIME'))
    def test_genericdatetime(self, dbe: DbEvaluator, func_name: str) -> None:
        assert dt_strip(dbe.eval('##2019-01-02 03:04:05##')) == datetime.datetime(2019, 1, 2, 3, 4, 5)
        assert dbe.eval('##2019-01-02##') == datetime.date(2019, 1, 2)
        assert dt_strip(dbe.eval(f'{func_name}(#2019-01-02 03:04:05#)')) == datetime.datetime(2019, 1, 2, 3, 4, 5)
        assert dt_strip(dbe.eval(f'{func_name}(#2019-01-02#)')) == to_datetime(datetime.date(2019, 1, 2))
        expected_values = (
            datetime.datetime(2019, 1, 2, 3, 4, 5),  # UTC CH
            datetime.datetime(2019, 1, 1, 22, 4, 5),  # NYC CH
        )
        assert dt_strip(dbe.eval(f'{func_name}("2019-01-02 03:04:05")')) == datetime.datetime(2019, 1, 2, 3, 4, 5)
        assert dt_strip(dbe.eval(f'{func_name}({int(utc_ts(2019, 1, 2, 3, 4, 5))})')) in expected_values
        assert dt_strip(dbe.eval(f'{func_name}({float(utc_ts(2019, 1, 2, 3, 4, 5))})')) in expected_values

    @pytest.mark.parametrize('func_name', ('GENERICDATETIME', 'DATETIME'))
    def test_genericdatetime_from_null(self, dbe: DbEvaluator, func_name: str) -> None:
        assert dbe.eval(f'{func_name}(NULL)') is None
        if not self.empty_str_is_null:  # NULL can be interpreted as ''
            assert dbe.eval(f'{func_name}(STR(NULL))') is None
        assert dbe.eval(f'{func_name}(DATE(NULL))') is None
        assert dbe.eval(f'{func_name}({func_name}(NULL))') is None

    @pytest.mark.parametrize('func_name', ('GENERICDATETIME', 'DATETIME'))
    def test_genericdatetime_from_data_null(self, dbe: DbEvaluator, null_data_table: sa.Table, func_name: str) -> None:
        if not self.empty_str_is_null:  # NULL can be interpreted as ''
            assert dbe.eval(f'{func_name}([str_null])', from_=null_data_table) is None
        assert dbe.eval(f'{func_name}([date_null])', from_=null_data_table) is None
        assert dbe.eval(f'{func_name}([datetime_null])', from_=null_data_table) is None


class DefaultGeopointTypeFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    def test_geopoint(self, dbe: DbEvaluator) -> None:
        assert to_str(dbe.eval('GEOPOINT("[55.75222,37.61556]")')) == '[55.75222,37.61556]'
        assert to_str(dbe.eval('GEOPOINT(GEOPOINT(55.75222, 37.61556))')) == '[55.75222,37.61556]'
        assert to_str(dbe.eval('GEOPOINT(55.75222, 37.61556)')) == '[55.75222,37.61556]'
        assert to_str(dbe.eval('GEOPOINT(55, 37)')) == '[55,37]'
        assert to_str(dbe.eval('GEOPOINT("55.75222", "37")')) == '[55.75222,37]'
        # Important case with values < 1
        assert to_str(dbe.eval('GEOPOINT("0.75222", "-0.37")')) == '[0.75222,-0.37]'

    def test_geopoint_from_null(self, dbe: DbEvaluator) -> None:
        if not self.empty_str_is_null:
            pytest.skip()

        assert dbe.eval('GEOPOINT(NULL)') is None
        assert dbe.eval('GEOPOINT(STR(NULL))') is None

    def test_geopoint_from_data_null(self, dbe: DbEvaluator, null_data_table: sa.Table) -> None:
        if not self.empty_str_is_null:
            pytest.skip()

        assert dbe.eval('GEOPOINT([str_null])', from_=null_data_table) is None


class DefaultGeopolygonTypeFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    def test_geopolygon(self, dbe: DbEvaluator) -> None:
        assert to_str(dbe.eval(
            'GEOPOLYGON("[[[55,37],[56,37],[56,38],[55,38]],[[50,37],[51,37],[51,38],[50,38]]]")'
        )) == '[[[55,37],[56,37],[56,38],[55,38]],[[50,37],[51,37],[51,38],[50,38]]]'

    def test_geopolygon_from_null(self, dbe: DbEvaluator) -> None:
        if not self.empty_str_is_null:
            pytest.skip()

        assert dbe.eval('GEOPOLYGON(NULL)') is None
        assert dbe.eval('GEOPOLYGON(STR(NULL))') is None

    def test_geopolygon_from_data_null(self, dbe: DbEvaluator, null_data_table: sa.Table) -> None:
        if not self.empty_str_is_null:
            pytest.skip()

        assert dbe.eval('GEOPOLYGON([str_null])', from_=null_data_table) is None


class DefaultDbCastTypeFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    pass


class DefaultTreeTypeFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    def test_tree_str(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.supports_arrays:
            raise NotImplementedError('Trees are not supported')

        assert to_str(dbe.eval('ARR_STR(TREE([arr_str_value]), "+")', from_=data_table)) == '++cde'
