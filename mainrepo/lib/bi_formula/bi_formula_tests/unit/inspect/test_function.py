from __future__ import annotations

from bi_formula.inspect.function import can_be_aggregate


def test_can_be_aggregate():
    assert can_be_aggregate(name="sum")
    assert not can_be_aggregate(name="greatest")
