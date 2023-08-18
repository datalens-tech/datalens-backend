from __future__ import annotations


def test_engine(sa_engine):
    conn = sa_engine.connect()
    assert conn
