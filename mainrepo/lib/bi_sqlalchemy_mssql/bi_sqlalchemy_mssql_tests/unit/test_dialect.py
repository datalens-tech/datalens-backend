from __future__ import annotations

import datetime
from decimal import Decimal
from unittest import mock
from uuid import UUID

import pyodbc
import pytest
from sqlalchemy import (
    Integer,
    String,
    column,
    select,
    sql,
    table,
)

from bi_sqlalchemy_mssql.base import BIMSSQLDialect


class TestDialect:
    __dialect__ = BIMSSQLDialect()

    def get_cursor(self):
        return mock.Mock(spec=pyodbc.Cursor)

    @pytest.mark.parametrize(
        "parameter, output",
        [
            (None, "NULL"),
            (True, "1"),
            (False, "0"),
            (5, "5"),
            (0.5, "0.5"),
            (Decimal("0.5"), "0.5"),
            (UUID("f9dad6af-eb1f-4d23-8b30-157eda50d8cd"), "N'f9dad6af-eb1f-4d23-8b30-157eda50d8cd'"),
            ("foobar", "N'foobar'"),
            (bytearray([1, 2, 3]), "0x010203"),
            (b"foo", "'foo'"),
            (b"foo\0bar", "0x666f6f00626172"),
            (datetime.datetime(2018, 5, 31, 4, 5, 6, 7000), "{ts '2018-05-31 04:05:06.007'}"),
            (datetime.date(2018, 5, 31), "{d '2018-05-31'}"),
            ((1,), "1"),
        ],
    )
    def test_quotes_parameter_types(self, parameter, output):
        assert self.__dialect__._quote_simple_value(parameter) == output

    def test_uses_custom_compiler(self):
        table1 = table(
            "mytable",
            column("myid", Integer),
            column("name", String),
            column("description", String),
        )

        q = select([table1.c.myid, sql.literal("bar").label("c1")], order_by=[table1.c.name + "-"]).alias("foo")
        crit = q.c.myid == table1.c.myid
        compiled = select(["*"], crit).compile(dialect=self.__dialect__)

        assert compiled.construct_params() == {"param_1": "bar"}

    def test_translates_single_element_tuple(self):
        cursor = self.get_cursor()
        with pytest.raises(AssertionError):
            self.__dialect__.do_execute(cursor, "foo", ((42,),))

    def test_substitutes_params_for_group_by(self):
        cursor = self.get_cursor()
        sql = "SELECT foo + ? FROM bar GROUP BY foo + ?"
        sql_check = "SELECT foo + 9 FROM bar GROUP BY foo + 10"
        params = (9, 10)
        self.__dialect__.do_execute(cursor, sql, params)

        cursor.execute.assert_called_once_with(sql_check, [])

    @mock.patch("bi_sqlalchemy_mssql.base.LOGGER", autospec=True, spec_set=True)
    def test_error_10004_logging(self, m_log):
        cursor = self.get_cursor()
        m_execute = mock.Mock()
        m_execute.side_effect = pyodbc.OperationalError()

        with mock.patch.object(cursor, "execute", m_execute):
            sql = "SELECT count(*) AS count_1"
            params = (1, 2, 3)
            with pytest.raises(AssertionError):
                self.__dialect__.do_execute(cursor, sql, params)
