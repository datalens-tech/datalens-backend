import datetime

import sqlalchemy as sa

from dl_connector_trino.core.adapters import CustomTrinoDialect


QUERY_TEMPLATE = """
SELECT
    t1.int,
    t1.string,
    t1.date,
    t1.datetime,
    t1.array_int,
    t1.array_string
FROM table t1
WHERE
    t1.int = {}
    AND t1.string = {}
    AND t1.date = {}
    AND t1.datetime = {}
    AND t1.array_int = {}
    AND t1.array_string = {}
"""

PARAMETERIZED_QUERY = QUERY_TEMPLATE.format(
    ":int_val",
    ":string_val",
    ":date_val",
    ":timestamp_val",
    ":array_int_val",
    ":array_string_val",
)

COMPILED_QUERY = QUERY_TEMPLATE.format(
    123,
    "'test_string'",
    "DATE '2025-06-18'",
    "TIMESTAMP '2025-06-18 15:40:03.000123'",
    "ARRAY[1, 2, 3]",
    "ARRAY['a', 'b', 'c']",
)


def test_custom_trino_compiler():
    """
    Test the CustomTrinoCompiler to ensure it correctly handles the custom compilation logic.
    """
    query = sa.text(PARAMETERIZED_QUERY).bindparams(
        sa.bindparam("int_val", value=123, type_=sa.BIGINT),
        sa.bindparam("string_val", value="test_string", type_=sa.String),
        sa.bindparam("date_val", value=datetime.date(2025, 6, 18), type_=sa.Date),
        sa.bindparam("timestamp_val", value=datetime.datetime(2025, 6, 18, 15, 40, 3, 123), type_=sa.DateTime),
        sa.bindparam("array_int_val", value=[1, 2, 3], type_=sa.ARRAY(sa.BIGINT)),
        sa.bindparam("array_string_val", value=["a", "b", "c"], type_=sa.ARRAY(sa.String)),
    )
    compiled_query = str(query.compile(dialect=CustomTrinoDialect(), compile_kwargs={"literal_binds": True}))
    assert compiled_query == COMPILED_QUERY, f"Expected: {COMPILED_QUERY}, but got: {compiled_query}"
