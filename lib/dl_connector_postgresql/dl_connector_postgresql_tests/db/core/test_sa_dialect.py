import datetime

import pytest
import pytz
import sqlalchemy as sa

from dl_connector_postgresql_tests.db.core.base import BasePostgreSQLTestClass


TEST_VALUES = [datetime.date(2020, 1, 1)] + [
    datetime.datetime(2020, idx1 + 1, idx2 + 1, 3, 4, 5, us).replace(tzinfo=tzinfo)
    for idx1, us in enumerate((0, 123356))
    for idx2, tzinfo in enumerate(
        (
            None,
            datetime.timezone.utc,
            pytz.timezone("America/New_York"),
        )
    )
]


class TestPostgresqlSaDialect(BasePostgreSQLTestClass):
    @pytest.mark.parametrize("value", TEST_VALUES, ids=[val.isoformat() for val in TEST_VALUES])
    def test_pg_literal_bind_datetimes(self, value, db):
        """
        Test that query results for literal_binds matches the query results without,
        for the custom dialect code.

        This test should be in the bi_postgresql dialect itself, but it doesn't have
        a postgres-using test at the moment.
        """
        execute = db.execute
        dialect = db._engine_wrapper.dialect

        query = sa.select([sa.literal(value)])
        compiled = str(query.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
        res_direct = list(execute(query))
        res_literal = list(execute(compiled))
        assert res_direct == res_literal, dict(literal_query=compiled)
