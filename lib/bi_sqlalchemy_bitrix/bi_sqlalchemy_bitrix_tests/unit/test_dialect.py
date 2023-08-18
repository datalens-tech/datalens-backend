from __future__ import annotations

import sqlalchemy as sa

from bi_sqlalchemy_bitrix.base import BitrixCompiler


def test_engine(sa_engine):
    assert sa_engine
    assert sa_engine.dialect


def test_select_compiler(sa_engine):
    query = sa.select([sa.column('COLUMN_ONE'), sa.column('COLUMN_TWO')]).where(
        sa.column('DATE_CREATE') == '2000-01-01',
    )
    query_compiled = str(BitrixCompiler(sa_engine.dialect, query))

    assert not query_compiled.startswith('SELECT *')
