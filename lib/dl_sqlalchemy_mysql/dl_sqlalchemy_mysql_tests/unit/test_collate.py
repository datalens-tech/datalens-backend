import pytest
import sqlalchemy as sa


def expr_to_text(expr, dialect, bind=True):
    compile_kwargs = {}
    if bind:
        compile_kwargs.update(literal_binds=True)
    expr_compiled = expr.compile(dialect=dialect, compile_kwargs=compile_kwargs)
    return str(expr_compiled)


metadata = sa.MetaData()
test_table = sa.Table("t1", metadata, sa.Column("k", sa.String(50)))


CI_EXPRS = [
    ("lower", sa.func.lower(sa.literal_column("'Ы'")).label("val_lc")),
    ("upper", sa.func.upper(sa.literal_column("'Ы'")).label("val_uc")),
    ("max", sa.func.max(test_table.c.k).label("val_max")),
    ("min", sa.func.min(test_table.c.k).label("val_min")),
    ("like", test_table.c.k.like("Müller%").label("val_like")),
    ("not_like", test_table.c.k.notlike("Müller%").label("val_not_like")),
]


CI_EXPECTED_WITH_COLLATE = [
    "SELECT lower('Ы' COLLATE utf8mb4_general_ci) AS val_lc",
    "SELECT upper('Ы' COLLATE utf8mb4_general_ci) AS val_uc",
    "SELECT max(t1.k COLLATE utf8mb4_general_ci) AS val_max\nFROM t1",
    "SELECT min(t1.k COLLATE utf8mb4_general_ci) AS val_min\nFROM t1",
    "SELECT t1.k LIKE 'Müller%%' COLLATE utf8mb4_general_ci AS val_like\nFROM t1",
    "SELECT t1.k NOT LIKE 'Müller%%' COLLATE utf8mb4_general_ci AS val_not_like\nFROM t1",
]


CI_EXPECTED_WITHOUT_COLLATE = [
    "SELECT lower('Ы') AS val_lc",
    "SELECT upper('Ы') AS val_uc",
    "SELECT max(t1.k) AS val_max\nFROM t1",
    "SELECT min(t1.k) AS val_min\nFROM t1",
    "SELECT t1.k LIKE 'Müller%%' AS val_like\nFROM t1",
    "SELECT t1.k NOT LIKE 'Müller%%' AS val_not_like\nFROM t1",
]


@pytest.fixture(
    params=list(zip(CI_EXPRS, CI_EXPECTED_WITH_COLLATE, CI_EXPECTED_WITHOUT_COLLATE)),
    ids=[name for name, _ in CI_EXPRS],
)
def ci_expr_with_expected(request):
    """Fixture that yields (expr, expected_with_collate, expected_without_collate)"""
    (name, expr), expected_ci, expected_no_ci = request.param
    yield expr, expected_ci, expected_no_ci


def test_collate_option(engine_url, ci_expr_with_expected):
    """
    Test that COLLATE is properly added to MySQL queries.

    MySQL behavior reference (from documentation):

        -- ORDER BY with collation
        SELECT k FROM t1 ORDER BY k COLLATE utf8mb4_general_ci;

        -- GROUP BY with collation
        SELECT k FROM t1 GROUP BY k COLLATE utf8mb4_general_ci;

        -- Aggregate functions with collation
        SELECT MAX(k COLLATE utf8mb4_general_ci) FROM t1;

        -- LIKE with collation
        SELECT * FROM t1 WHERE k LIKE _latin1 'Müller' COLLATE utf8mb4_general_ci;

    """
    ci_expr, expected_ci, expected_no_ci = ci_expr_with_expected
    ci_expr = sa.select([ci_expr])
    eng_ci = sa.create_engine(engine_url, enforce_collate="utf8mb4_general_ci")
    eng = sa.create_engine(engine_url)
    sql_ci = expr_to_text(ci_expr, eng_ci.dialect)
    sql = expr_to_text(ci_expr, eng.dialect)
    assert sql != sql_ci
    assert " COLLATE " in sql_ci
    assert " COLLATE " not in sql
    assert sql_ci == expected_ci, f"With COLLATE: expected {expected_ci!r}, got {sql_ci!r}"
    assert sql == expected_no_ci, f"Without COLLATE: expected {expected_no_ci!r}, got {sql!r}"
    print()
    print(sql_ci)
    print(sql)


def test_order_by_collate(engine_url):
    """Test ORDER BY with COLLATE"""
    query = sa.select([test_table.c.k]).order_by(test_table.c.k)
    eng_ci = sa.create_engine(engine_url, enforce_collate="utf8mb4_general_ci")
    eng = sa.create_engine(engine_url)
    sql_ci = expr_to_text(query, eng_ci.dialect, bind=False)
    sql = expr_to_text(query, eng.dialect, bind=False)

    assert "ORDER BY" in sql_ci
    assert " COLLATE utf8mb4_general_ci" in sql_ci
    assert " COLLATE " not in sql
    assert sql_ci == "SELECT t1.k\nFROM t1\n ORDER BY t1.k COLLATE utf8mb4_general_ci"
    assert sql == "SELECT t1.k\nFROM t1\n ORDER BY t1.k"
    print()
    print(f"ORDER BY with COLLATE: {sql_ci}")
    print(f"ORDER BY without COLLATE: {sql}")


def test_group_by_collate(engine_url):
    """Test GROUP BY with COLLATE"""
    query = sa.select([test_table.c.k, sa.func.count()]).group_by(test_table.c.k)
    eng_ci = sa.create_engine(engine_url, enforce_collate="utf8mb4_general_ci")
    eng = sa.create_engine(engine_url)
    sql_ci = expr_to_text(query, eng_ci.dialect, bind=False)
    sql = expr_to_text(query, eng.dialect, bind=False)

    assert "GROUP BY" in sql_ci
    assert " COLLATE utf8mb4_general_ci" in sql_ci
    assert " COLLATE " not in sql
    assert sql_ci == "SELECT t1.k, count(*) AS count_1\nFROM t1\n GROUP BY t1.k COLLATE utf8mb4_general_ci"
    assert sql == "SELECT t1.k, count(*) AS count_1\nFROM t1\n GROUP BY t1.k"
    print()
    print(f"GROUP BY with COLLATE: {sql_ci}")
    print(f"GROUP BY without COLLATE: {sql}")
