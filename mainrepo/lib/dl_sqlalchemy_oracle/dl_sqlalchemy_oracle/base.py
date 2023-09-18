from __future__ import annotations

import sys

import oracledb

oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb

import sqlalchemy as sa  # noqa: E402
from sqlalchemy.dialects.oracle.cx_oracle import OracleDialect_cx_oracle as UPSTREAM  # noqa: E402

from dl_sqlalchemy_common.base import CompilerPrettyMixin  # noqa: E402


class BIOracleCompilerBasic(UPSTREAM.statement_compiler):
    """Necessary overrides"""


class BIOracleCompiler(BIOracleCompilerBasic, UPSTREAM.statement_compiler, CompilerPrettyMixin):
    """Added prettification"""

    def limit_clause(self, *args, **kwargs):
        # upstream: `return ""`; no prettification to be done.
        return super().limit_clause(*args, **kwargs)

    def visit_join(self, join, from_linter=None, **kwargs):
        # superclass customizes join for the `not self.dialect.use_ansi` case,
        # which isn't currently supported in the prettifier anyway.
        return super().visit_join(join, from_linter=from_linter, **kwargs)


class BIOracleDialectBasic(UPSTREAM):
    statement_compiler = BIOracleCompilerBasic


class BIOracleDialect(BIOracleDialectBasic):
    statement_compiler = BIOracleCompiler


def register_dialect():
    sa.dialects.registry.register("bi_oracle_basic", "dl_sqlalchemy_oracle.base", "BIOracleDialectBasic")
    sa.dialects.registry.register("bi_oracle", "dl_sqlalchemy_oracle.base", "BIOracleDialect")
