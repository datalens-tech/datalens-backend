from __future__ import annotations

import pytest
import sqlalchemy as sa


def expr_to_text(expr, dialect, bind=True):
    compile_kwargs = {}
    if bind:
        compile_kwargs.update(literal_binds=True)
    expr_compiled = expr.compile(dialect=dialect, compile_kwargs=compile_kwargs)
    return str(expr_compiled)


CI_EXPRS = [
    ("ilike", sa.literal_column("'Ы'").ilike("ы").label("val")),
    ("lower", sa.func.lower(sa.literal_column("'Ы'")).label("val_lc")),
    ("upper", sa.func.upper(sa.literal_column("'Ы'")).label("val_uc")),
]


@pytest.fixture(params=CI_EXPRS, ids=[name for name, _ in CI_EXPRS])
def ci_expr(request):
    _, expr = request.param
    yield expr


@pytest.mark.param
def test_collate_option(engine_url, ci_expr):
    """
    ...

    Postgresql behavior reference (with default collate `C`):

        => SELECT 'Ы' ILIKE 'ы' AS val;
        val | f

        => SELECT 'Ы' ILIKE 'ы' COLLATE "en_US" AS val;
        val | t

        => SELECT 'Ы' ILIKE ('ы' COLLATE "en_US") AS val;
        val | t

        => SELECT 'Ы' ILIKE lower('ы' COLLATE "en_US") AS val;
        val | t

        => select lower('Ы');
        lower | Ы

        => select lower('Ы' COLLATE "en_US");
        lower | ы
    """
    ci_expr = sa.select([ci_expr])
    eng_ci = sa.create_engine(engine_url, enforce_collate="en_US")
    eng = sa.create_engine(engine_url)
    sql_ci = expr_to_text(ci_expr, eng_ci.dialect)
    sql = expr_to_text(ci_expr, eng.dialect)
    assert sql != sql_ci
    assert " COLLATE " in sql_ci
    assert " COLLATE " not in sql
    print()
    print(sql_ci)
    print(sql)
