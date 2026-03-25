import sqlalchemy as sa
from sqlalchemy.dialects.mysql.base import (
    MySQLDDLCompiler,
    MySQLTypeCompiler,
)

from dl_sqlalchemy_mysql.base import DLMYSQLDialect


class StarRocksTypeCompiler(MySQLTypeCompiler):
    def visit_DATETIME(self, type_, **kw):
        return "DATETIME"

    def visit_BOOLEAN(self, type_, **kw):
        return "BOOLEAN"


class StarRocksDDLCompiler(MySQLDDLCompiler):
    def post_create_table(self, table):
        try:
            first_col = next(iter(table.columns))
        except StopIteration:
            raise sa.exc.CompileError(
                "StarRocksDDLCompiler.post_create_table() "
                "cannot generate DUPLICATE KEY clause for a table with no columns"
            )
        quoted_col_name = self.preparer.format_column(first_col)
        return f"\nDUPLICATE KEY({quoted_col_name})"


class BIStarRocksDialect(DLMYSQLDialect):
    name = "bi_starrocks"
    type_compiler = StarRocksTypeCompiler
    ddl_compiler = StarRocksDDLCompiler


def register_dialect():
    sa.dialects.registry.register("bi_starrocks", "dl_sqlalchemy_starrocks.base", "BIStarRocksDialect")
