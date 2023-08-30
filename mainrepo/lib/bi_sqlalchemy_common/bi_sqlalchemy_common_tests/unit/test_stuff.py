from __future__ import annotations

from bi_sqlalchemy_common.base import CompilerPrettyMixin


def test_inherit():
    """ Ensure mid-point inheritance works """
    from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2 as UPSTREAM

    class CustomPGCompiler(UPSTREAM.statement_compiler):
        """ Local customizations """

    class ActualCustomPGCompiler(CustomPGCompiler, CompilerPrettyMixin):
        """ Local customization on top of pretty-formatting """

        def limit_clause(self, *args, **kwargs):
            """
            This method has to be overridden because otherwise
            `UPSTREAM.statement_compiler.limit_clause` overrides the
            `CompilerPrettyMixin.limit_clause`, and there's a check to catch
            that.
            """
            return super().limit_clause(*args, **kwargs)

    assert ActualCustomPGCompiler
