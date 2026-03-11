"""SQLAlchemy dialect registration for StarRocks (bi_starrocks scheme)."""

from sqlalchemy.dialects.mysql.base import (
    MySQLDDLCompiler,
    MySQLTypeCompiler,
)

from dl_sqlalchemy_mysql.base import DLMYSQLDialect


class StarRocksTypeCompiler(MySQLTypeCompiler):
    """Type compiler that handles StarRocks-specific type differences."""

    def visit_DATETIME(self, type_, **kw):
        """
        StarRocks doesn't support precision for DATETIME type.
        Override to use DATETIME without precision.
        """
        return "DATETIME"

    def visit_BOOLEAN(self, type_, **kw):
        """StarRocks uses BOOLEAN, not BOOL."""
        return "BOOLEAN"


class StarRocksDDLCompiler(MySQLDDLCompiler):
    """DDL compiler that adds DUPLICATE KEY clause required by StarRocks."""

    def post_create_table(self, table):
        first_col = list(table.columns.keys())[0]
        return f"\nDUPLICATE KEY(`{first_col}`)"


class BiStarRocksDialect(DLMYSQLDialect):
    """
    SQLAlchemy dialect for bi_starrocks:// URL scheme.

    StarRocks is MySQL-compatible, so we inherit from DLMYSQLDialect.
    """

    name = "bi_starrocks"
    type_compiler = StarRocksTypeCompiler
    ddl_compiler = StarRocksDDLCompiler
