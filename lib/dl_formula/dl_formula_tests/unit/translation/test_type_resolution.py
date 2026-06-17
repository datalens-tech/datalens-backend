from __future__ import annotations

from dl_formula.core.datatype import DataType
from dl_formula.definitions.type_strategy import (
    Fixed,
    FromArgs,
)


def test_strategy_fixed():
    strat = Fixed(DataType.STRING)
    assert strat.get_from_args([DataType.INTEGER, DataType.DATETIME]) == DataType.STRING
    assert strat.get_from_args([DataType.STRING]) == DataType.STRING


def test_strategy_from_args():
    strat = FromArgs(slice(1, None))
    assert strat.get_from_args([DataType.DATETIME, DataType.INTEGER]) == DataType.INTEGER
    assert (
        strat.get_from_args([DataType.DATETIME, DataType.INTEGER, DataType.FLOAT, DataType.INTEGER]) == DataType.FLOAT
    )
