from __future__ import annotations

import decimal
from typing import (
    Callable,
    ClassVar,
    List,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

import pytest
import sqlalchemy as sa

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.base import FormulaConnectorTestBase


NUMERIC = Union[decimal.Decimal, int, float]


def approx(value):  # type: ignore  # 2024-01-29 # TODO: Function is missing a type annotation  [no-untyped-def]
    if isinstance(value, list):
        result = []
        for subvalue in value:
            if isinstance(subvalue, decimal.Decimal):
                subvalue = float(subvalue)
            result.append(subvalue)
        value = result
    return pytest.approx(value, rel=1e-4)


def avg(values: List[NUMERIC]) -> NUMERIC:
    return sum(values) / len(values)


class DefaultWindowFunctionFormulaConnectorTestSuite(FormulaConnectorTestBase):
    supports_rank_percentile: ClassVar[bool] = True

    def test_rank(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        assert values == list(range(0, 100, 10)) + list(range(0, 100, 10))

        assert dbe.eval("RANK([int_value])", from_=data_table, order_by=["[id]"], many=True) == (
            [19 - val * 2 for val in range(10)] + [19 - val * 2 for val in range(10)]
        )
        assert dbe.eval('RANK([int_value], "asc")', from_=data_table, order_by=["[id]"], many=True) == (
            [val * 2 + 1 for val in range(10)] + [val * 2 + 1 for val in range(10)]
        )
        assert dbe.eval('RANK(ABS(30 - [int_value]), "asc")', from_=data_table, order_by=["[id]"], many=True) == (
            [11, 7, 3, 1, 3, 7, 11, 15, 17, 19, 11, 7, 3, 1, 3, 7, 11, 15, 17, 19]
        )
        assert dbe.eval('RANK([id], "asc" WITHIN [int_value])', from_=data_table, order_by=["[id]"], many=True) == (
            [1] * 10 + [2] * 10
        )

    def test_rank_dense(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        assert values == list(range(0, 100, 10)) + list(range(0, 100, 10))

        assert dbe.eval("RANK_DENSE([int_value])", from_=data_table, order_by=["[id]"], many=True) == (
            [10 - val for val in range(10)] + [10 - val for val in range(10)]
        )
        assert dbe.eval('RANK_DENSE([int_value], "asc")', from_=data_table, order_by=["[id]"], many=True) == (
            [val + 1 for val in range(10)] + [val + 1 for val in range(10)]
        )
        assert dbe.eval('RANK_DENSE(ABS(30 - [int_value]), "asc")', from_=data_table, order_by=["[id]"], many=True) == (
            [4, 3, 2, 1, 2, 3, 4, 5, 6, 7, 4, 3, 2, 1, 2, 3, 4, 5, 6, 7]
        )
        assert dbe.eval(
            'RANK_DENSE([id], "asc" WITHIN [int_value])', from_=data_table, order_by=["[id]"], many=True
        ) == ([1] * 10 + [2] * 10)

    def test_rank_unique(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        assert values == list(range(0, 100, 10)) + list(range(0, 100, 10))

        assert dbe.eval("RANK_UNIQUE([id])", from_=data_table, order_by=["[id]"], many=True) == list(
            reversed(range(1, 21))
        )
        assert dbe.eval('RANK_UNIQUE([id], "asc")', from_=data_table, order_by=["[id]"], many=True) == list(
            range(1, 21)
        )
        assert set(dbe.eval("RANK_UNIQUE([int_value])", from_=data_table, order_by=["[id]"], many=True)) == set(
            range(1, 21)
        )
        assert set(dbe.eval('RANK_UNIQUE([int_value], "asc")', from_=data_table, order_by=["[id]"], many=True)) == set(
            range(1, 21)
        )
        assert set(
            dbe.eval('RANK_UNIQUE(ABS(30 - [int_value]), "asc")', from_=data_table, order_by=["[id]"], many=True)
        ) == set(range(1, 21))
        assert dbe.eval(
            'RANK_UNIQUE([id], "asc" WITHIN [int_value])', from_=data_table, order_by=["[id]"], many=True
        ) == ([1] * 10 + [2] * 10)

    def test_rank_percentile(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        if not self.supports_rank_percentile:
            pytest.skip()

        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        cnt = len(values)
        assert values == list(range(0, 100, 10)) + list(range(0, 100, 10))

        assert approx(
            dbe.eval('RANK_PERCENTILE([int_value], "asc")', from_=data_table, order_by=["[id]"], many=True)
        ) == ([(val * 2) / (cnt - 1) for val in range(10)] + [(val * 2) / (cnt - 1) for val in range(10)])

    def test_sum(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        assert values == list(range(0, 100, 10)) + list(range(0, 100, 10))
        assert dbe.eval("SUM([int_value] TOTAL)", from_=data_table, order_by=["[id]"], many=True) == (
            [sum(values)] * 20
        )
        assert dbe.eval("SUM([id] WITHIN [int_value])", from_=data_table, order_by=["[id]"], many=True) == (
            [12 + val * 2 for val in range(10)] + [12 + val * 2 for val in range(10)]
        )

    def test_min(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        assert values == list(range(0, 100, 10)) + list(range(0, 100, 10))
        assert dbe.eval("MIN([int_value] TOTAL)", from_=data_table, order_by=["[id]"], many=True) == (
            [min(values)] * 20
        )
        assert dbe.eval("MIN([id] WITHIN [int_value])", from_=data_table, order_by=["[id]"], many=True) == (
            [1 + val for val in range(10)] + [1 + val for val in range(10)]
        )

    def test_max(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        assert values == list(range(0, 100, 10)) + list(range(0, 100, 10))
        assert dbe.eval("MAX([int_value] TOTAL)", from_=data_table, order_by=["[id]"], many=True) == (
            [max(values)] * 20
        )
        assert dbe.eval("MAX([id] WITHIN [int_value])", from_=data_table, order_by=["[id]"], many=True) == (
            [11 + val for val in range(10)] + [11 + val for val in range(10)]
        )

    def test_count(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        assert values == list(range(0, 100, 10)) + list(range(0, 100, 10))
        # 1 arg
        assert dbe.eval("COUNT([int_value] TOTAL)", from_=data_table, order_by=["[id]"], many=True) == (
            [len(values)] * 20
        )
        assert dbe.eval("COUNT([id] WITHIN [int_value])", from_=data_table, order_by=["[id]"], many=True) == (
            [2 for _ in range(10)] + [2 for _ in range(10)]
        )
        # 0 args
        assert dbe.eval("COUNT(TOTAL)", from_=data_table, order_by=["[id]"], many=True) == ([len(values)] * 20)
        assert dbe.eval("COUNT(WITHIN [int_value])", from_=data_table, order_by=["[id]"], many=True) == (
            [2 for _ in range(10)] + [2 for _ in range(10)]
        )

    def test_avg(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        assert values == list(range(0, 100, 10)) + list(range(0, 100, 10))
        assert approx(dbe.eval("AVG([int_value] TOTAL)", from_=data_table, order_by=["[id]"], many=True)) == (
            [sum(values) / 20] * 20
        )
        assert approx(dbe.eval("AVG([id] WITHIN [int_value])", from_=data_table, order_by=["[id]"], many=True)) == (
            [6 + val for val in range(10)] + [6 + val for val in range(10)]
        )

    def test_sum_if(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        assert values == list(range(0, 100, 10)) + list(range(0, 100, 10))
        assert dbe.eval("SUM_IF([int_value], [id] < 11 TOTAL)", from_=data_table, order_by=["[id]"], many=True) == (
            [sum(v for id_0, v in enumerate(values) if id_0 + 1 < 11)] * 20  # (id_0 = id - 1)
        )
        assert dbe.eval(
            "SUM_IF([id], [id] < 11 WITHIN [int_value])", from_=data_table, order_by=["[id]"], many=True
        ) == ([val for val in range(1, 11)] + [val for val in range(1, 11)])

    def test_count_if(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        assert values == list(range(0, 100, 10)) + list(range(0, 100, 10))
        assert dbe.eval("COUNT_IF([int_value], [id] < 11 TOTAL)", from_=data_table, order_by=["[id]"], many=True) == (
            [10] * 20
        )
        assert dbe.eval(
            "COUNT_IF([id], [id] < 11 WITHIN [int_value])", from_=data_table, order_by=["[id]"], many=True
        ) == ([1] * 10 + [1] * 10)

    def test_avg_if(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        assert values == list(range(0, 100, 10)) + list(range(0, 100, 10))
        assert dbe.eval(
            "AVG_IF([int_value], [id] < 11 TOTAL)", from_=data_table, order_by=["[id]"], many=True
        ) == pytest.approx(
            [sum(v for id_0, v in enumerate(values) if id_0 + 1 < 11) / 10.0] * 20  # (id_0 = id - 1)
        )
        assert dbe.eval(
            "AVG_IF([id], [id] < 11 WITHIN [int_value])", from_=data_table, order_by=["[id]"], many=True
        ) == pytest.approx([val / 1.0 for val in range(1, 11)] + [val for val in range(1, 11)])

    def _check_rfunc(
        self, dbe: DbEvaluator, data_table: sa.Table, func_name: str, py_agg_func: Callable[[List[NUMERIC]], NUMERIC]
    ) -> None:
        ids = list(range(1, 21))
        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        assert values == list(range(0, 100, 10)) + list(range(0, 100, 10))
        assert approx(dbe.eval(f"{func_name}([int_value] TOTAL)", from_=data_table, order_by=["[id]"], many=True)) == (
            [py_agg_func(values[: i + 1]) for i in range(20)]
        )
        assert approx(
            dbe.eval(f"{func_name}([id] WITHIN [int_value])", from_=data_table, order_by=["[id]"], many=True)
        ) == ([py_agg_func([ids[i]]) for i in range(10)] + [py_agg_func([ids[i], ids[i + 10]]) for i in range(10)])
        assert approx(
            dbe.eval(f'{func_name}([int_value], "desc" TOTAL)', from_=data_table, order_by=["[id]"], many=True)
        ) == ([py_agg_func(values[i:]) for i in range(20)])
        assert approx(
            dbe.eval(
                f'{func_name}([int_value], "asc" TOTAL ORDER BY [id] DESC)',
                from_=data_table,
                order_by=["[id]"],
                many=True,
            )
        ) == ([py_agg_func(values[i:]) for i in range(20)])
        assert approx(
            dbe.eval(f'{func_name}([id], "desc" WITHIN [int_value])', from_=data_table, order_by=["[id]"], many=True)
        ) == ([py_agg_func([ids[i], ids[i + 10]]) for i in range(10)] + [py_agg_func([ids[i]]) for i in range(10, 20)])

    def test_rsum(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        self._check_rfunc(dbe, data_table, func_name="RSUM", py_agg_func=sum)

    def test_rcount(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        self._check_rfunc(dbe, data_table, func_name="RCOUNT", py_agg_func=len)

    def test_rmin(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        self._check_rfunc(dbe, data_table, func_name="RMIN", py_agg_func=min)

    def test_rmax(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        self._check_rfunc(dbe, data_table, func_name="RMAX", py_agg_func=max)

    def test_ravg(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        self._check_rfunc(dbe, data_table, func_name="RAVG", py_agg_func=avg)

    def _check_mfunc(
        self, dbe: DbEvaluator, data_table: sa.Table, func_name: str, py_agg_func: Callable[[List[NUMERIC]], NUMERIC]
    ) -> None:
        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        assert values == list(range(0, 100, 10)) + list(range(0, 100, 10))
        assert approx(dbe.eval(f"{func_name}([int_value], 1)", from_=data_table, order_by=["[id]"], many=True)) == (
            [py_agg_func(values[max(0, i - 1) : i + 1]) for i in range(20)]
        )
        assert approx(
            dbe.eval(f"{func_name}([int_value], 1 ORDER BY [id] DESC)", from_=data_table, order_by=["[id]"], many=True)
        ) == ([py_agg_func(values[i : i + 2]) for i in range(20)])
        assert approx(dbe.eval(f"{func_name}([int_value], -1)", from_=data_table, order_by=["[id]"], many=True)) == (
            [py_agg_func(values[i : i + 2]) for i in range(20)]
        )
        assert approx(dbe.eval(f"{func_name}([int_value], 0 - 1)", from_=data_table, order_by=["[id]"], many=True)) == (
            [py_agg_func(values[i : i + 2]) for i in range(20)]
        )
        assert approx(dbe.eval(f"{func_name}([int_value], 5, 5)", from_=data_table, order_by=["[id]"], many=True)) == (
            [py_agg_func(values[max(0, i - 5) : i + 6]) for i in range(20)]
        )

    def test_msum(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        self._check_mfunc(dbe, data_table, func_name="MSUM", py_agg_func=sum)

    def test_mcount(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        self._check_mfunc(dbe, data_table, func_name="MCOUNT", py_agg_func=len)

    def test_mmin(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        self._check_mfunc(dbe, data_table, func_name="MMIN", py_agg_func=min)

    def test_mmax(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        self._check_mfunc(dbe, data_table, func_name="MMAX", py_agg_func=max)

    def test_mavg(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        self._check_mfunc(dbe, data_table, func_name="MAVG", py_agg_func=avg)

    def test_lag(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        VALUE_TV = TypeVar("VALUE_TV")

        def lag(
            values: Sequence[VALUE_TV],
            idx: int,
            offset: int = 1,
            default: Optional[VALUE_TV] = None,
        ) -> Optional[VALUE_TV]:
            offset_idx = idx - offset
            if offset_idx < 0 or offset_idx >= len(values):
                return default
            return values[offset_idx]

        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        assert values == list(range(0, 100, 10)) + list(range(0, 100, 10))
        assert dbe.eval("LAG([int_value])", from_=data_table, order_by=["[id]"], many=True) == (
            [lag(values, i, offset=1) for i in range(20)]
        )
        assert dbe.eval("LAG([int_value] ORDER BY [id] DESC)", from_=data_table, order_by=["[id]"], many=True) == (
            [lag(values, i, offset=-1) for i in range(20)]
        )
        assert dbe.eval("LAG([int_value], -2)", from_=data_table, order_by=["[id]"], many=True) == (
            [lag(values, i, offset=-2) for i in range(20)]
        )
        assert dbe.eval("LAG([int_value], 2)", from_=data_table, order_by=["[id]"], many=True) == (
            [lag(values, i, offset=2) for i in range(20)]
        )
        # With default
        assert dbe.eval("LAG([int_value], -3, 8)", from_=data_table, order_by=["[id]"], many=True) == (
            [lag(values, i, offset=-3, default=8) for i in range(20)]
        )
        assert dbe.eval("LAG([int_value], 3, 8)", from_=data_table, order_by=["[id]"], many=True) == (
            [lag(values, i, offset=3, default=8) for i in range(20)]
        )

    def test_first(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        assert values == list(range(0, 100, 10)) + list(range(0, 100, 10))

        assert dbe.eval("FIRST([int_value] TOTAL ORDER BY [id])", from_=data_table, many=True) == [values[0]] * len(
            values
        )
        assert dbe.eval("FIRST([int_value] TOTAL ORDER BY [id] DESC)", from_=data_table, many=True) == [
            values[-1]
        ] * len(values)

    def test_last(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        values = data_table.int_values  # type: ignore  # 2024-01-29 # TODO: "Table" has no attribute "int_values"  [attr-defined]
        assert values == list(range(0, 100, 10)) + list(range(0, 100, 10))

        assert dbe.eval("LAST([int_value] TOTAL ORDER BY [id])", from_=data_table, many=True) == [values[-1]] * len(
            values
        )
        assert dbe.eval("LAST([int_value] TOTAL ORDER BY [id] DESC)", from_=data_table, many=True) == [values[0]] * len(
            values
        )
