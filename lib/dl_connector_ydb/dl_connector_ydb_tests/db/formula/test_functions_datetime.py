import datetime
import typing

import pytest
import sqlalchemy as sa

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_datetime import DefaultDateTimeFunctionFormulaConnectorTestSuite
from dl_formula_testing.util import utc_ts

from dl_connector_ydb_tests.db.formula.base import YQLTestBase


def dt_avg(values: typing.Iterable[datetime.datetime]) -> datetime.datetime:
    tses = [utc_ts(dt) for dt in values]
    ts = sum(tses) / len(tses)
    # Note: remove microseconds as currently not implemented
    return datetime.datetime.utcfromtimestamp(ts).replace(microsecond=0)


def dt_max(values: typing.Iterable[datetime.datetime]) -> datetime.datetime:
    tses = [utc_ts(dt) for dt in values]
    ts = max(tses)
    return datetime.datetime.utcfromtimestamp(ts)


class TestDateTimeFunctionYQL(YQLTestBase, DefaultDateTimeFunctionFormulaConnectorTestSuite):
    supports_datepart_2_non_const = False

    def test_datetime_functions(
        self, dbe: DbEvaluator, ydb_data_table: sa.Table, ydb_data_table_field_types_patch
    ) -> None:
        assert dbe.eval("AVG([datetime_value])", from_=ydb_data_table) == dt_avg(ydb_data_table.datetime_values)
        assert dbe.eval("MAX([datetime_value])", from_=ydb_data_table) == dt_max(ydb_data_table.datetime_values)
        assert dbe.eval("MAX(DATETIME([datetime_value]))", from_=ydb_data_table) == dt_max(
            ydb_data_table.datetime_values
        )
        assert dbe.eval("MAX(INT([datetime_value]))", from_=ydb_data_table) == int(
            dt_max(ydb_data_table.datetime_values).replace(tzinfo=datetime.timezone.utc).timestamp()
        )
        assert dbe.eval("MAX(FLOAT([datetime_value]))", from_=ydb_data_table) == float(
            int(dt_max(ydb_data_table.datetime_values).replace(tzinfo=datetime.timezone.utc).timestamp())
        )

    @pytest.mark.xfail
    def test_timestamp_functions(
        self, dbe: DbEvaluator, ydb_data_table: sa.Table, ydb_data_table_field_types_patch
    ) -> None:
        assert dbe.eval("AVG([timestamp_value])", from_=ydb_data_table) == dt_avg(ydb_data_table.timestamp_values)
        assert dbe.eval("MAX([timestamp_value])", from_=ydb_data_table) == dt_max(ydb_data_table.timestamp_values)
        assert dbe.eval("MAX(DATETIME([timestamp_value]))", from_=ydb_data_table) == dt_max(
            ydb_data_table.timestamp_values
        )
        assert dbe.eval("MAX(INT([timestamp_value]))", from_=ydb_data_table) == int(
            dt_max(ydb_data_table.timestamp_values).replace(tzinfo=datetime.timezone.utc).timestamp()
        )
        assert dbe.eval("MAX(FLOAT([timestamp_value]))", from_=ydb_data_table) == float(
            int(dt_max(ydb_data_table.timestamp_values).replace(tzinfo=datetime.timezone.utc).timestamp())
        )
