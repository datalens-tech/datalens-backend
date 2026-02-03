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

CI_EXPECTED_WITH_COLLATE = [
    "SELECT 'Ы' ILIKE 'ы' COLLATE \"en_US\" AS val",
    "SELECT lower('Ы' COLLATE \"en_US\") AS val_lc",
    "SELECT upper('Ы' COLLATE \"en_US\") AS val_uc",
]

CI_EXPECTED_WITHOUT_COLLATE = [
    "SELECT 'Ы' ILIKE 'ы' AS val",
    "SELECT lower('Ы') AS val_lc",
    "SELECT upper('Ы') AS val_uc",
]


@pytest.fixture(
    params=list(zip(CI_EXPRS, CI_EXPECTED_WITH_COLLATE, CI_EXPECTED_WITHOUT_COLLATE)),
    ids=[name for name, _ in CI_EXPRS],
)
def ci_expr_with_expected(request):
    """Fixture that yields (expr, expected_with_collate, expected_without_collate)"""
    (name, expr), expected_ci, expected_no_ci = request.param
    yield expr, expected_ci, expected_no_ci


@pytest.mark.param
def test_collate_option(engine_url, ci_expr_with_expected):
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
    ci_expr, expected_ci, expected_no_ci = ci_expr_with_expected
    ci_expr = sa.select([ci_expr])
    eng_ci = sa.create_engine(engine_url, enforce_collate="en_US")
    eng = sa.create_engine(engine_url)
    sql_ci = expr_to_text(ci_expr, eng_ci.dialect)
    sql = expr_to_text(ci_expr, eng.dialect)
    assert sql != sql_ci
    assert " COLLATE " in sql_ci
    assert " COLLATE " not in sql
    assert sql_ci == expected_ci
    assert sql == expected_no_ci
    print()
    print(sql_ci)
    print(sql)
