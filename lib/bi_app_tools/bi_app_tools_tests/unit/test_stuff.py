from __future__ import annotations

from bi_app_tools.entry_point import BIEntryPoint


def test_some():
    ep = BIEntryPoint()
    assert ep.usage
