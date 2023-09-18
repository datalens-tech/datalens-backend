from __future__ import annotations

import sqlalchemy as sa

import dl_sqlalchemy_chyt.types as chyt_types
from dl_sqlalchemy_clickhouse.base import BIClickHouseDialect as UPSTREAM

quote_identifier = UPSTREAM().identifier_preparer.quote


# Type converters
ischema_names = {
    **UPSTREAM.ischema_names,
    "YtBoolean": chyt_types.YtBoolean,
}


class CHYTTableExpression(sa.sql.elements.TextClause):
    @staticmethod
    def _quote_identifier(value):
        return quote_identifier(value)

    @staticmethod
    def _quote_value(value):
        from clickhouse_sqlalchemy.drivers.http.escaper import Escaper

        return Escaper().escape_item(value)
        # return QUOTER.quote(value)

    def __init__(self, text, alias=None, **kwargs):
        if alias:
            text = "{} as {}".format(text, self._quote_identifier(alias))
        text = text.replace(":", r"\:")  # see also: bi_core.utils.sa_plain_text
        super(CHYTTableExpression, self).__init__(text, **kwargs)


class CHYTTablesConcat(CHYTTableExpression):
    def __init__(self, *tables, **kwargs):
        text = "concatYtTables({})".format(", ".join(self._quote_identifier(table) for table in tables if table))
        self._tables = tables
        super(CHYTTablesConcat, self).__init__(text, **kwargs)


class CHYTTablesRange(CHYTTableExpression):
    def __init__(self, directory, start=None, end=None, **kwargs):
        if end is not None:
            args = [directory, start or "", end]
        elif start is not None:
            args = [directory, start]
        else:
            args = [directory]

        text = "concatYtTablesRange({})".format(", ".join(self._quote_value(arg) for arg in args))
        self._directory = directory
        self._start = start
        self._end = end
        super(CHYTTablesRange, self).__init__(text, **kwargs)


class CHYTTableSubselect(CHYTTableExpression):
    def __init__(self, subsql, **kwargs):
        text = "(\n{}\n)".format(subsql)
        self._subsql = subsql
        super(CHYTTableSubselect, self).__init__(text, **kwargs)


class BICHYTTypeCompiler(UPSTREAM.type_compiler):
    def visit_ytboolean(self, type_, **kw):
        return "YtBoolean"


class BICHYTDialect(UPSTREAM):
    # Add YT-only types
    ischema_names = ischema_names
    type_compiler = BICHYTTypeCompiler

    def get_columns(self, connection, table_name, schema=None, **kw):
        if not isinstance(table_name, (CHYTTableExpression, sa.sql.elements.TextClause)):
            table_name = self.identifier_preparer.quote_identifier(table_name)

        query = "DESCRIBE TABLE {}".format(table_name)
        rows = self._execute(connection, query)

        return [self._get_column_info(row.name, row.type) for row in rows]


def register_dialect():
    sa.dialects.registry.register("bi_chyt", "dl_sqlalchemy_chyt.base", "BICHYTDialect")
