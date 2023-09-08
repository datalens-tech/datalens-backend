from __future__ import annotations

import pytest
import sqlalchemy as sa
from sqlalchemy import Column, Integer, MetaData, String, Table, desc
from sqlalchemy.sql import column, table

# Reference:
# https://github.com/sqlalchemy/sqlalchemy/blob/rel_1_4/test/sql/test_compiler.py#L107


table1 = table(
    "mytable",
    column("myid", Integer),
    column("name", String),
    column("description", String),
)

table2 = table(
    "myothertable", column("otherid", Integer), column("othername", String)
)

table3 = table(
    "thirdtable", column("userid", Integer), column("otherstuff", String)
)

metadata = MetaData()

# table with a schema
table4 = Table(
    "remotetable",
    metadata,
    Column("rem_id", Integer, primary_key=True),
    Column("datatype_id", Integer),
    Column("value", String(20)),
    schema="remote_owner",
)

# table with a "multipart" schema
table5 = Table(
    "remotetable",
    metadata,
    Column("rem_id", Integer, primary_key=True),
    Column("datatype_id", Integer),
    Column("value", String(20)),
    schema="dbo.remote_owner",
)


t1a = table1.alias("t1a")
t2a = table2.alias("t2a")
t2b = table2.alias("t2b")


query1 = (
    t1a
    .join(t2a, t1a.c.myid == t2a.c.otherid, full=True)
    .outerjoin(t2b, t1a.c.myid == t2b.c.otherid, full=True)
    .select()
    # Note: currently this doesn't get wrapped.
    .where(t1a.c.myid > -1).where(t1a.c.myid < 9000)
    # ... and this does.
    .where(sa.and_(
        (t1a.c.name != letter * 8)
        for letter in "abcde"
    ))
    # Postgresql does `limit all`, other dialects raise: `.offset(123)`
)
sq1 = query1.subquery().alias("q_1")

t4a = table4.alias("t4a")
t5a = table5.alias("t5a")
t5b = table5.alias("t5b")
query2 = (
    sq1
    .join(t4a, sq1.c.myid == t4a.c.rem_id)
    .join(t5a, t4a.c.datatype_id == t5a.c.datatype_id)
    .join(t5b, sq1.c.name == t5b.c.value)
    .select()
    .where(sq1.c.name != "wxyz")
    .order_by(
        desc(sq1.c.myid), t4a.c.datatype_id, sq1.c.name, desc(sq1.c.description),
        desc(sq1.c.otherid), desc(sq1.c.othername), t4a.c.datatype_id, t4a.c.value,
        desc(t5a.c.rem_id), desc(t5a.c.datatype_id), desc(t5a.c.value),
        desc(t5b.c.rem_id), desc(t5b.c.datatype_id), desc(t5b.c.value),
    )
    .limit(int("12345" * 10))
    .offset(int("54321" * 10))
)


def get_dialects():
    try:
        from bi_sqlalchemy_chyt.base import BICHYTDialect
        yield BICHYTDialect()
    except ImportError:
        pass
    try:
        from bi_sqlalchemy_clickhouse.base import BIClickHouseDialect
        yield BIClickHouseDialect()
    except ImportError:
        pass
    try:
        from bi_sqlalchemy_mssql.base import BIMSSQLDialect
        yield BIMSSQLDialect()
    except ImportError:
        pass
    try:
        from bi_sqlalchemy_oracle.base import BIOracleDialect
        yield BIOracleDialect()
    except ImportError:
        pass
    try:
        from bi_sqlalchemy_postgres import BIPGDialect
        yield BIPGDialect()
    except ImportError:
        pass


DIALECTS = tuple(get_dialects())


@pytest.fixture(
    scope="session",
    params=DIALECTS,
    ids=[dialect.__class__.__name__ for dialect in DIALECTS],
)
def dialect(request):
    return request.param


@pytest.fixture(
    scope="session",
    params=[False, True],
    ids=["literal_binds=False", "literal_binds=True"],
)
def literal_binds(request):
    return request.param


@pytest.fixture(
    scope="session",
    params=[query1, query2],
    ids=["query1", "query2"],
)
def query(request):
    return request.param


def test_compile(dialect, query, literal_binds):
    sql_text = str(query.compile(
        dialect=dialect,
        compile_kwargs=dict(literal_binds=literal_binds),
    ))
    assert sql_text
    max_width = max(len(row) for row in sql_text.splitlines())
    assert max_width < 9000
    print()
    print(f"{max_width=!r}")
    print("Query:\n    " + sql_text.replace("\n", "\n    ") + "\n")
