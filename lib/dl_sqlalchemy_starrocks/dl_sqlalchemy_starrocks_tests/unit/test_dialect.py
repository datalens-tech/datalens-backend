import sqlalchemy as sa
from sqlalchemy.dialects import mysql as mysql_types


def test_engine(sa_engine):
    assert sa_engine
    assert sa_engine.dialect
    assert sa_engine.dialect.name == "bi_starrocks"


def test_datetime_no_precision(sa_engine):
    """StarRocks DATETIME must not include precision like DATETIME(n)."""
    table = sa.Table("t", sa.MetaData(), sa.Column("ts", mysql_types.DATETIME))
    ddl = str(sa.schema.CreateTable(table).compile(dialect=sa_engine.dialect))
    assert "DATETIME" in ddl
    assert "DATETIME(" not in ddl


def test_boolean_type(sa_engine):
    """StarRocks uses BOOLEAN, not BOOL."""
    table = sa.Table("t", sa.MetaData(), sa.Column("flag", mysql_types.BOOLEAN))
    ddl = str(sa.schema.CreateTable(table).compile(dialect=sa_engine.dialect))
    assert "BOOLEAN" in ddl


def test_ddl_duplicate_key(sa_engine):
    """StarRocks DDL must include DUPLICATE KEY on first column."""
    table = sa.Table("t", sa.MetaData(), sa.Column("id", mysql_types.BIGINT))
    ddl = str(sa.schema.CreateTable(table).compile(dialect=sa_engine.dialect))
    assert "DUPLICATE KEY(`id`)" in ddl
