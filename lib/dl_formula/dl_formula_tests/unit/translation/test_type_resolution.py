from __future__ import annotations

from dl_formula.core.datatype import DataType as DT
from dl_formula.definitions.type_strategy import (
    Fixed,
    FromArgs,
)


def test_strategy_fixed():
    strat = Fixed(DT.STRING)
    assert strat.get_from_args([DT.INTEGER, DT.DATETIME]) == DT.STRING
    assert strat.get_from_args([DT.STRING]) == DT.STRING


def test_strategy_from_args():
    strat = FromArgs(slice(1, None))
    assert strat.get_from_args([DT.DATETIME, DT.INTEGER]) == DT.INTEGER
    assert strat.get_from_args([DT.DATETIME, DT.INTEGER, DT.FLOAT, DT.INTEGER]) == DT.FLOAT
