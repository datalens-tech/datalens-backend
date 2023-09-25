from __future__ import annotations


def test_engine(sa_engine):
    assert sa_engine
    assert sa_engine.dialect
