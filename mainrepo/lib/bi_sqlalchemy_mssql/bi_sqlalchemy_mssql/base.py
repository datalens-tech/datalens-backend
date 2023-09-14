from __future__ import annotations

import binascii
import datetime
import decimal
import itertools
import logging
import uuid

import pyodbc
import sqlalchemy as sa
import sqlalchemy.dialects.mssql.base as upbase
from sqlalchemy.dialects.mssql.pyodbc import MSDialect_pyodbc as UPSTREAM

from bi_sqlalchemy_common.base import CompilerPrettyMixin

LOGGER = logging.getLogger(__name__)


class BIMSSQLDialectBasic(UPSTREAM):
    @staticmethod  # noqa: C901
    def _quote_simple_value(value):
        """Mainly from pymssql quoting, without the encoded output"""

        if value is None:
            return "NULL"

        if isinstance(value, bool):
            return "1" if value else "0"

        if isinstance(value, float):
            return repr(value)

        if isinstance(value, (int, decimal.Decimal)):
            return str(value)

        if isinstance(value, uuid.UUID):
            return BIMSSQLDialect._quote_simple_value(str(value))

        if isinstance(value, str):
            return "N'" + value.replace("'", "''") + "'"

        if isinstance(value, bytearray):
            return "0x" + binascii.hexlify(bytes(value)).decode()

        if isinstance(value, bytes):
            # see if it can be decoded as ascii if there are no null bytes
            if b"\0" not in value:
                try:
                    value.decode("ascii")
                    return "'" + value.replace(b"'", b"''").decode() + "'"
                except UnicodeDecodeError:
                    pass

            # Python 3: handle bytes
            # @todo - Marc - hack hack hack
            if isinstance(value, bytes):
                return (b"0x" + binascii.hexlify(value)).decode()

            # will still be string type if there was a null byte in it or if the
            # decoding failed.  In this case, just send it as hex.
            if isinstance(value, str):
                return "0x" + value.encode("hex")

        if isinstance(value, datetime.datetime):
            return "{ts '%04d-%02d-%02d %02d:%02d:%02d.%03d'}" % (
                value.year,
                value.month,
                value.day,
                value.hour,
                value.minute,
                value.second,
                value.microsecond / 1000,
            )

        if isinstance(value, datetime.date):
            return "{d '%04d-%02d-%02d'}" % (value.year, value.month, value.day)

        if isinstance(value, tuple) and len(value) == 1:
            # Sometimes a result of a query will be provided as a filter to another query, but
            # the object passed may not be scalar. pymssql does some flattening on this, and
            # we should too.
            return BIMSSQLDialect._quote_simple_value(value[0])

        return None

    @staticmethod
    def translate_custom_parameters(params):
        def translate(param):
            if isinstance(param, tuple):
                # Sometimes a result of a query will be provided as a filter to another query, but
                # the object passed may not be scalar. pymssql does some flattening on this, and
                # we should too.
                return param[0]
            return param

        return [translate(param) for param in params]

    def roll_parameters_into_statement(self, statement, parameters):
        assert statement.count("?") == len(parameters), "num of placeholders != num of params"

        # transform here and pass on to cursor
        quoted_params = [self._quote_simple_value(param) for param in parameters]
        statement_list = statement.split("?")
        # Most efficient method I found for getting the lists put together. Assumes that
        # the number of parameters actually matches the number of ? placeholders.
        return "".join(
            [x for x in itertools.chain.from_iterable(itertools.zip_longest(statement_list, quoted_params)) if x]
        )

    def do_execute(self, cursor, statement, parameters, context=None):
        if parameters:
            statement = self.roll_parameters_into_statement(statement, parameters)
            # no need for parameters at this point, since they're all baked into the query
            parameters = tuple()

        try:
            cursor.execute(statement, self.translate_custom_parameters(parameters))
        except pyodbc.OperationalError:
            LOGGER.error(
                "pyodbc OperationalError. Full statement: {}\n Params: {}".format(
                    statement,
                    parameters,
                )
            )
            raise

    @upbase._db_plus_owner
    def has_table(self, connection, tablename, dbname, owner, schema):
        self._ensure_has_table_connection(connection)
        if tablename.startswith("#"):  # temporary table
            tables = upbase.ischema.mssql_temp_table_columns

            s = upbase.sql.select(tables.c.table_name).where(
                tables.c.table_name.like(self._temp_table_name_like_pattern(tablename))
            )

            result = connection.execute(s.limit(1))
            return result.scalar() is not None
        else:
            tables = upbase.ischema.tables

            s = upbase.sql.select(tables.c.table_name).where(
                upbase.sql.and_(
                    # Original: `tables.c.table_type == "BASE TABLE",`
                    upbase.sql.or_(
                        tables.c.table_type == "BASE TABLE",
                        tables.c.table_type == "VIEW",
                    ),
                    # ...
                    tables.c.table_name == tablename,
                )
            )

            if owner:
                s = s.where(tables.c.table_schema == owner)

            c = connection.execute(s)

            return c.first() is not None


class BIMSSQLCompiler(UPSTREAM.statement_compiler, CompilerPrettyMixin):
    def order_by_clause(self, select, **kw):
        sup = super().order_by_clause(select, **kw)
        return self._pretty.postprocess_block("ORDER BY", sup)

    def limit_clause(self, cs, **kw):
        # Upstream: `return ""`
        return super().limit_clause(cs, **kw)

    def visit_alias(self, alias, **kw):
        # Upstream only wraps its superclass.
        return super().visit_alias(alias, **kw)


class BIMSSQLDialect(BIMSSQLDialectBasic):
    statement_compiler = BIMSSQLCompiler


def register_dialect():
    sa.dialects.registry.register("bi_mssql_basic", "bi_sqlalchemy_mssql.base", "BIMSSQLDialectBasic")
    sa.dialects.registry.register("bi_mssql", "bi_sqlalchemy_mssql.base", "BIMSSQLDialect")
