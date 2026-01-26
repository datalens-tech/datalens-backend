import sqlalchemy as sa

import dl_sqlalchemy_ydb.dialect as ydb_dialect


def test_cast_utf8_type(sa_engine: sa.engine.Engine):
    query = sa.sql.select(sa.sql.cast("test_string", ydb_dialect.YqlUtf8))

    compiled_sql = str(query.compile(dialect=sa_engine.dialect, compile_kwargs={"literal_binds": True}))

    assert "CAST('test_string' AS Utf8)" in compiled_sql


def test_cast_double_type(sa_engine: sa.engine.Engine):
    query = sa.sql.select(sa.sql.cast(42.5, ydb_dialect.YqlDouble))

    compiled_sql = str(query.compile(dialect=sa_engine.dialect, compile_kwargs={"literal_binds": True}))

    assert "CAST(42.5 AS Double)" in compiled_sql


def test_cast_uuid_type(sa_engine: sa.engine.Engine):
    query = sa.sql.select(sa.sql.cast("617ddd51-7960-4e61-8509-32fa03dccb41", ydb_dialect.YqlUuid))

    compiled_sql = str(query.compile(dialect=sa_engine.dialect, compile_kwargs={"literal_binds": True}))

    assert "CAST('617ddd51-7960-4e61-8509-32fa03dccb41' AS Uuid)" in compiled_sql


def test_cast_from_column_to_utf8(sa_engine: sa.engine.Engine):
    metadata = sa.MetaData()
    test_table = sa.Table(
        "test_table",
        metadata,
        sa.Column("text_col", sa.Text),
    )

    query = sa.sql.select(sa.sql.cast(test_table.c.text_col, ydb_dialect.YqlUtf8))

    compiled_sql = str(query.compile(dialect=sa_engine.dialect, compile_kwargs={"literal_binds": True}))

    assert "CAST(test_table.text_col AS Utf8)" in compiled_sql


def test_cast_from_column_to_double(sa_engine: sa.engine.Engine):
    metadata = sa.MetaData()
    test_table = sa.Table(
        "test_table",
        metadata,
        sa.Column("numeric_col", sa.Float),
    )

    query = sa.sql.select(sa.sql.cast(test_table.c.numeric_col, ydb_dialect.YqlDouble))

    compiled_sql = str(query.compile(dialect=sa_engine.dialect, compile_kwargs={"literal_binds": True}))

    assert "CAST(test_table.numeric_col AS Double)" in compiled_sql


def test_cast_from_column_to_uuid(sa_engine: sa.engine.Engine):
    metadata = sa.MetaData()
    test_table = sa.Table(
        "test_table",
        metadata,
        sa.Column("uuid_col", sa.Float),
    )

    query = sa.sql.select(sa.sql.cast(test_table.c.uuid_col, ydb_dialect.YqlUuid))

    compiled_sql = str(query.compile(dialect=sa_engine.dialect, compile_kwargs={"literal_binds": True}))

    assert "CAST(test_table.uuid_col AS Uuid)" in compiled_sql
