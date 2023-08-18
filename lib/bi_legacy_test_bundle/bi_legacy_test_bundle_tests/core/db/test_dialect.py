from __future__ import annotations

import datetime

import sqlalchemy as sa
import sqlalchemy.dialects.mysql
import sqlalchemy.sql.sqltypes
import pytest
import pytz


TEST_VALUES = [datetime.date(2020, 1, 1)] + [
    datetime.datetime(2020, idx1 + 1, idx2 + 1, 3, 4, 5, us).replace(tzinfo=tzinfo)
    for idx1, us in enumerate((0, 123356))
    for idx2, tzinfo in enumerate((
        None,
        datetime.timezone.utc,
        pytz.timezone('America/New_York'),
    ))
]


@pytest.mark.parametrize("value", TEST_VALUES, ids=[val.isoformat() for val in TEST_VALUES])
def test_pg_literal_bind_datetimes(value, postgres_db):
    """
    Test that query results for literal_binds matches the query results without,
    for the custom dialect code.

    This test should be in the bi_postgresql dialect itself, but it doesn't have
    a postgres-using test at the moment.
    """
    db = postgres_db
    execute = db.execute
    dialect = db._engine_wrapper.dialect

    query = sa.select([sa.literal(value)])
    compiled = str(query.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
    res_direct = list(execute(query))
    res_literal = list(execute(compiled))
    assert res_direct == res_literal, dict(literal_query=compiled)


@pytest.mark.parametrize(
    ("value", "type_", "expected"),
    (
        pytest.param(
            datetime.date(2022, 1, 2),
            sqlalchemy.sql.sqltypes.Date(),
            datetime.date(2022, 1, 2),
            id='date-as-date'
        ),
        pytest.param(
            '2022-01-02',
            sqlalchemy.dialects.mysql.DATE(),
            datetime.date(2022, 1, 2),
            id='date-as-string'
        ),
        pytest.param(
            datetime.datetime(2022, 1, 2, 12, 59, 59),
            sqlalchemy.sql.sqltypes.DateTime(),
            datetime.datetime(2022, 1, 2, 12, 59, 59),
            id='datetime-as-datetime'
        ),
        pytest.param(
            '2022-01-02T12:59:59',
            sqlalchemy.dialects.mysql.DATETIME(fsp=6),
            datetime.datetime(2022, 1, 2, 12, 59, 59),
            id='datetime-as-string'
        ),
    ),
)
def test_mysql_literal_bind_datetimes(value, type_, expected, mysql_db):
    db = mysql_db
    execute = db.execute
    dialect = db._engine_wrapper.dialect

    query = sa.select([sa.literal(value, type_=type_)])
    compiled = str(query.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
    res_literal = list(execute(compiled))
    assert res_literal[0][0] == expected
