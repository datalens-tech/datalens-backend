import datetime

import pytest
import sqlalchemy as sa
import sqlalchemy.dialects.mysql
import sqlalchemy.sql.sqltypes

from dl_connector_mysql_tests.db.core.base import BaseMySQLTestClass


class TestMySQLSaDialect(BaseMySQLTestClass):
    @pytest.mark.parametrize(
        ("value", "type_", "expected"),
        (
            pytest.param(
                datetime.date(2022, 1, 2), sqlalchemy.sql.sqltypes.Date(), datetime.date(2022, 1, 2), id="date-as-date"
            ),
            pytest.param(
                "2022-01-02", sqlalchemy.dialects.mysql.DATE(), datetime.date(2022, 1, 2), id="date-as-string"
            ),
            pytest.param(
                datetime.datetime(2022, 1, 2, 12, 59, 59),
                sqlalchemy.sql.sqltypes.DateTime(),
                datetime.datetime(2022, 1, 2, 12, 59, 59),
                id="datetime-as-datetime",
            ),
            pytest.param(
                "2022-01-02T12:59:59",
                sqlalchemy.dialects.mysql.DATETIME(fsp=6),
                datetime.datetime(2022, 1, 2, 12, 59, 59),
                id="datetime-as-string",
            ),
        ),
    )
    def test_mysql_literal_bind_datetimes(self, value, type_, expected, db):
        execute = db.execute
        dialect = db._engine_wrapper.dialect

        query = sa.select([sa.literal(value, type_=type_)])
        compiled = str(query.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
        res_literal = list(execute(compiled))
        assert res_literal[0][0] == expected
