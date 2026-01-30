"""SQLAlchemy dialect registration for StarRocks (bi_starrocks scheme)."""

from sqlalchemy.dialects.mysql.base import MySQLTypeCompiler
from sqlalchemy.types import DATETIME

from dl_sqlalchemy_mysql.base import DLMYSQLDialect


class StarRocksTypeCompiler(MySQLTypeCompiler):
    """Type compiler that handles StarRocks-specific type differences."""

    def visit_DATETIME(self, type_, **kw):
        """
        StarRocks doesn't support precision for DATETIME type.
        Override to use DATETIME without precision.
        """
        return "DATETIME"


class BiStarRocksDialect(DLMYSQLDialect):
    """
    SQLAlchemy dialect for bi_starrocks:// URL scheme.

    StarRocks is MySQL-compatible, so we inherit from DLMYSQLDialect.
    """

    name = "bi_starrocks"
    type_compiler = StarRocksTypeCompiler
