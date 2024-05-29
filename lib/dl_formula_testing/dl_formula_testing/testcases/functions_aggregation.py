from __future__ import annotations

import datetime
from typing import (
    ClassVar,
    Collection,
    TypeVar,
    Union,
)

import pytest
import sqlalchemy as sa

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.base import FormulaConnectorTestBase
from dl_formula_testing.util import utc_ts


class DefaultMainAggFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    supports_countd_approx: ClassVar[bool] = False
    supports_quantile: ClassVar[bool] = False
    supports_median: ClassVar[bool] = False
    supports_arg_min_max: ClassVar[bool] = False
    supports_any: ClassVar[bool] = False
    supports_all_concat: ClassVar[bool] = False
    supports_top_concat: ClassVar[bool] = False

    def test_basic_aggregation_functions(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]

        assert dbe.eval("SUM([int_value])", from_=data_table) == sum(values)
        assert dbe.eval("AVG([int_value])", from_=data_table) == sum(values) / len(values)
        assert dbe.eval("MAX([int_value])", from_=data_table) == max(values)
        assert dbe.eval("MIN([int_value])", from_=data_table) == min(values)
        assert dbe.eval("COUNT()", from_=data_table) == len(values)
        assert dbe.eval("COUNT([int_value])", from_=data_table) == len(values)
        assert dbe.eval("COUNTD([int_value])", from_=data_table) == len(set(values))

    def test_date_avg_function(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        date_values = data_table.date_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "date_values"  [attr-defined]

        assert dbe.eval("AVG([date_value])", from_=data_table) == datetime.date.fromtimestamp(
            sum(map(lambda date: utc_ts(date), date_values)) / len(date_values)
        )

    def test_datetime_avg_function(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        datetime_values = data_table.datetime_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "datetime_values"  [attr-defined]

        def dt_avg(values) -> datetime.datetime:  # type: ignore  # 2024-01-29 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
            tses = [utc_ts(dt) for dt in values]
            ts = sum(tses) / len(tses)
            return datetime.datetime.utcfromtimestamp(ts)

        # Sanity check:
        assert dt_avg(datetime_values[:1]) == datetime_values[0]
        result = dbe.eval("AVG([datetime_value])", from_=data_table)
        # XXX: shouldn't this have `astimezone` first?
        result = result.replace(tzinfo=None)
        assert result == dt_avg(datetime_values)

        # To make sure that the everchanging DT types all work:
        result = dbe.eval("AVG(DATETIME([datetime_value]))", from_=data_table)
        result = result.replace(tzinfo=None)
        assert result == dt_avg(datetime_values)
        result = dbe.eval("AVG(GENERICDATETIME([datetime_value]))", from_=data_table)
        result = result.replace(tzinfo=None)
        assert result == dt_avg(datetime_values)

    def test_statistical_aggregation_functions(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]

        avg = sum(values) / len(values)
        var_samp = sum([(v - avg) ** 2 for v in values]) / (len(values) - 1)
        assert dbe.eval("VAR([int_value])", from_=data_table) == pytest.approx(var_samp)
        assert dbe.eval("STDEV([int_value])", from_=data_table) == pytest.approx(var_samp**0.5)
        var_pop = sum([(v - avg) ** 2 for v in values]) / len(values)
        assert dbe.eval("VARP([int_value])", from_=data_table) == pytest.approx(var_pop)
        assert dbe.eval("STDEVP([int_value])", from_=data_table) == pytest.approx(var_pop**0.5)

    def test_approximate_aggregation_functions(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.supports_countd_approx:
            pytest.skip()

        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        assert dbe.eval("COUNTD_APPROX([int_value])", from_=data_table) == len(set(values))

    def test_conditional_aggregation_functions(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]

        assert dbe.eval("SUM_IF([int_value], [int_value] > 5)", from_=data_table) == sum([v for v in values if v > 5])
        assert dbe.eval("SUM_IF([int_value] + 2 * 2, [int_value] > 5)", from_=data_table) == sum(
            [v + 2 * 2 for v in values if v > 5]
        )

        assert dbe.eval("AVG_IF([int_value], [int_value] > 5)", from_=data_table) == (lambda x: sum(x) / len(x))(
            [v for v in values if v > 5]
        )
        assert dbe.eval("AVG_IF([int_value] + 2 * 2, [int_value] > 5)", from_=data_table) == (
            lambda x: sum(x) / len(x)
        )([v + 2 * 2 for v in values if v > 5])

        # Zero matching elements
        assert (
            dbe.eval(
                "AVG_IF([int_value], [int_value] > {max_val} and [int_value] < {min_val})".format(
                    max_val=max(values), min_val=min(values)
                ),
                from_=data_table,
            )
            is None
        )

        assert dbe.eval("COUNT_IF([int_value] > 5)", from_=data_table) == len([v for v in values if v > 5])
        assert dbe.eval("COUNT_IF([int_value] + 1 > 5)", from_=data_table) == len([v for v in values if (v + 1) > 5])
        assert dbe.eval("COUNT_IF([int_value] > 5)", from_=data_table, where="1 = 0") == 0

        assert dbe.eval("COUNTD_IF([int_value], [int_value] > 5)", from_=data_table) == len(
            {v for v in values if v > 5}
        )
        assert dbe.eval("COUNTD_IF([int_value] % 2, [int_value] - 1 > 5)", from_=data_table) == len(
            {v % 2 for v in values if (v - 1) > 5}
        )

    def test_quantile(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.supports_quantile:
            pytest.skip()

        value = dbe.eval("QUANTILE([int_value], 0.9)", from_=data_table)
        assert 80 <= value <= 90

    def test_median(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.supports_median:
            pytest.skip()

        int_values = data_table.int_values  # type: ignore  # 2024-05-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        date_values = data_table.date_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "date_values"  [attr-defined]
        datetime_values = data_table.datetime_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "datetime_values"  [attr-defined]

        VALUE_TV = TypeVar("VALUE_TV")

        def median(values: Collection[Union[VALUE_TV]]) -> VALUE_TV:
            values = sorted(values)  # type: ignore  # 2024-01-30 # TODO: Value of type variable "SupportsRichComparisonT" of "sorted" cannot be "VALUE_TV"  [type-var]
            upper_middle = len(values) // 2 + 1
            return values[upper_middle]

        assert dbe.eval("MEDIAN([int_value])", from_=data_table) == median(int_values)
        assert dbe.eval("MEDIAN([date_value])", from_=data_table) == median(date_values)
        assert dbe.eval("MEDIAN([datetime_value])", from_=data_table) == median(datetime_values)

    def test_any(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.supports_any:
            pytest.skip()

        int_values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        assert dbe.eval("ANY([int_value])", from_=data_table) in int_values

    def test_arg_min_max(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.supports_arg_min_max:
            pytest.skip()

        assert dbe.eval('ARG_MIN(CONCAT("q_", [int_value]), (50-[int_value])^2)', from_=data_table) == "q_50"
        assert dbe.eval('ARG_MAX(CONCAT("q_", [int_value]), 50-[int_value]^2)', from_=data_table) == "q_0"

    def test_all_concat(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.supports_all_concat:
            pytest.skip()

        expected_values = sorted(str(val) for val in set(data_table.int_values))  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        assert dbe.eval("ALL_CONCAT([int_value])", from_=data_table) == ", ".join(expected_values)
        assert dbe.eval("ALL_CONCAT([int_value], '')", from_=data_table) == "".join(expected_values)
        assert dbe.eval("ALL_CONCAT([int_value], ' – ')", from_=data_table) == " – ".join(expected_values)

    def test_top_concat(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.supports_top_concat:
            pytest.skip()

        expected_values = set(str(val) for val in set(data_table.int_values))  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        res = dbe.eval("TOP_CONCAT([int_value], 3)", from_=data_table)
        assert not set(res.split(", ")) - expected_values, "should be a subset"
        res = dbe.eval("TOP_CONCAT([int_value], 3, '; ')", from_=data_table)
        assert not set(res.split("; ")) - expected_values, "should be a subset"
