from __future__ import annotations

import json

from bi_api_lib.utils.json_streaming import FakeJSONList


def test_fakejsonlist_empty():
    fjl = FakeJSONList()
    value = ''
    value += fjl.finalize()
    assert json.loads(value) == []


def test_fakejsonlist_a():
    fjl = FakeJSONList()
    value = ''
    fjl.feed('11')
    value += fjl.flush()
    value += fjl.finalize()
    assert json.loads(value) == [11]


def test_fakejsonlist_b():
    fjl = FakeJSONList()
    value = ''
    value += fjl.flush()
    fjl.feed('11')
    value += fjl.finalize()
    assert json.loads(value) == [11]


def test_fakejsonlist_c():
    fjl = FakeJSONList()
    value = ''
    fjl.feed('11')
    fjl.feed('22')
    value += fjl.flush()
    fjl.feed('33')
    fjl.feed('44')
    value += fjl.finalize()
    assert json.loads(value) == [11, 22, 33, 44]
