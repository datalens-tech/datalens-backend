import sqlalchemy as sa

from dl_sqlalchemy_postgres import AsyncBIPGDialect
from dl_sqlalchemy_postgres.asyncpg import DBAPIMock

from dl_connector_postgresql.core.postgresql_base.utils import compile_pg_query


def _compile_string_literal_preview_query(exclude_types: frozenset) -> tuple[str, list]:
    """Compile a query that mirrors what the preview endpoint generates for a constant string field."""
    dialect = AsyncBIPGDialect()
    # Mirrors: SELECT "some_string" AS res_0, <other cols> FROM t LIMIT 10
    # Simplified to the problematic part: a string literal in a SELECT output column with LIMIT
    query = sa.select(sa.literal("some_string").label("res_0")).limit(10)
    return compile_pg_query(query, dialect, exclude_types=exclude_types)


def test_greenplum_produces_bare_param_without_type() -> None:
    """Documents the bug: when STRING is excluded (current behavior shared with PG),
    the string literal compiles to a bare $1 with no type annotation.
    Greenplum then fails PREPARE with IndeterminateDatatypeError."""
    # This is exactly what AsyncPostgresAdapter._execute_by_step currently passes
    # for both PostgreSQL AND Greenplum — the source of the bug
    current_exclude_types = frozenset({DBAPIMock.ENUM, DBAPIMock.STRING})
    compiled, params = _compile_string_literal_preview_query(current_exclude_types)

    assert "$1" in compiled
    assert (
        "::varchar" not in compiled
    ), "bare $1 without ::varchar is what causes IndeterminateDatatypeError on Greenplum PREPARE"
    assert "some_string" in params


def test_greenplum_fix_produces_varchar_cast() -> None:
    """Greenplum does not exclude STRING from type annotations.
    The string literal then compiles to $1::varchar, which Greenplum can type-check
    during PREPARE without error."""
    # STRING removed from exclude_types — this is the desired Greenplum behavior
    fixed_exclude_types = frozenset({DBAPIMock.ENUM})
    compiled, params = _compile_string_literal_preview_query(fixed_exclude_types)

    assert "$1::varchar" in compiled, "$1::varchar cast is required so Greenplum PREPARE can resolve the parameter type"
    assert "some_string" in params


def test_enum_column_filter_stays_bare_with_greenplum_fix() -> None:
    """Parameters bound to native ENUM-typed columns remain bare even after removing STRING
    from exclude_types.

    DL maps PostgreSQL's native ENUM type to UserDataType.string internally, which raised
    a question: could removing STRING from exclude_types cause ENUM-column filter parameters
    to receive ::varchar annotations?

    Answer: no. SQLAlchemy assigns DBAPIMock.ENUM (not DBAPIMock.STRING) to parameters bound
    against ENUM-typed columns, and ENUM remains excluded in AsyncGreenplumAdapter.
    The ::varchar cast only affects untyped string literals in SELECT output columns — which
    is exactly the bug being fixed — not parameters that flow through an Enum column type."""

    dialect = AsyncBIPGDialect()
    greenplum_exclude_types = frozenset({DBAPIMock.ENUM})

    table = sa.table("t", sa.column("status", sa.Enum("active", "inactive", name="status_enum")))
    query = sa.select(sa.literal(1).label("x")).where(table.c.status == "active").limit(5)
    compiled, params = compile_pg_query(query, dialect, exclude_types=greenplum_exclude_types)

    # ENUM-typed parameter stays bare — DBAPIMock.ENUM is still excluded
    assert "::enum" not in compiled, "::enum would fail if the type is not registered with asyncpg"
    assert "::varchar" not in compiled, "ENUM params are excluded as ENUM, not recast to varchar"
    assert "active" in params


def test_non_string_params_keep_their_casts_with_fixed_exclude_types() -> None:
    """Sanity check: removing STRING from exclude_types does not affect other type casts."""
    dialect = AsyncBIPGDialect()
    fixed_exclude_types = frozenset({DBAPIMock.ENUM})
    query = sa.select(sa.literal(42).label("int_col"), sa.literal(3.14).label("float_col")).limit(5)
    compiled, params = compile_pg_query(query, dialect, exclude_types=fixed_exclude_types)

    # Integer and float params still have their casts
    assert any(cast in compiled for cast in ("::integer", "::smallint", "::bigint")), compiled
    assert "::double precision" in compiled or "::float" in compiled or "::numeric" in compiled, compiled
    assert 42 in params
    assert 3.14 in params
