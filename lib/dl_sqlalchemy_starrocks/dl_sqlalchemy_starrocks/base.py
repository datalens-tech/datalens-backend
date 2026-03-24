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
        first_col = list(table.columns.keys())[0]
        return f"\nDUPLICATE KEY(`{first_col}`)"


class BiStarRocksDialect(DLMYSQLDialect):
    name = "bi_starrocks"
    type_compiler = StarRocksTypeCompiler
    ddl_compiler = StarRocksDDLCompiler


def register_dialect():
    sa.dialects.registry.register("bi_starrocks", "dl_sqlalchemy_starrocks.base", "BiStarRocksDialect")
