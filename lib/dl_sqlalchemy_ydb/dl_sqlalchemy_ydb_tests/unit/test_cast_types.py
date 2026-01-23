import sqlalchemy as sa

import dl_sqlalchemy_ydb.cast_types as ydb_cast_types


def test_cast_utf8_type(sa_engine: sa.engine.Engine):
    query = sa.sql.select(sa.sql.cast("test_string", ydb_cast_types.Utf8))

    compiled_sql = str(query.compile(dialect=sa_engine.dialect, compile_kwargs={"literal_binds": True}))

    assert "CAST('test_string' AS Utf8)" in compiled_sql


def test_cast_double_type(sa_engine: sa.engine.Engine):
    query = sa.sql.select(sa.sql.cast(42.5, ydb_cast_types.Double))

    compiled_sql = str(query.compile(dialect=sa_engine.dialect, compile_kwargs={"literal_binds": True}))

    assert "CAST(42.5 AS Double)" in compiled_sql


def test_cast_from_column_to_utf8(sa_engine: sa.engine.Engine):
    metadata = sa.MetaData()
    test_table = sa.Table(
        "test_table",
        metadata,
        sa.Column("text_col", sa.Text),
    )

    query = sa.sql.select(sa.sql.cast(test_table.c.text_col, ydb_cast_types.Utf8))

    compiled_sql = str(query.compile(dialect=sa_engine.dialect, compile_kwargs={"literal_binds": True}))

    assert "CAST(test_table.text_col AS Utf8)" in compiled_sql


def test_cast_from_column_to_double(sa_engine: sa.engine.Engine):
    metadata = sa.MetaData()
    test_table = sa.Table(
        "test_table",
        metadata,
        sa.Column("numeric_col", sa.Float),
    )

    query = sa.sql.select(sa.sql.cast(test_table.c.numeric_col, ydb_cast_types.Double))

    compiled_sql = str(query.compile(dialect=sa_engine.dialect, compile_kwargs={"literal_binds": True}))

    assert "CAST(test_table.numeric_col AS Double)" in compiled_sql
