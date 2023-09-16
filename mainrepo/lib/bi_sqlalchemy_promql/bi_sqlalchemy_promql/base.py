from sqlalchemy.dialects import registry
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler


class PromQLCompiler(compiler.SQLCompiler):
    def visit_sequence(self, sequence, **kw):
        pass

    def visit_empty_set_expr(self, element_types):
        pass

    def update_from_clause(self, update_stmt, from_table, extra_froms, from_hints, **kw):
        pass

    def delete_extra_from_clause(self, update_stmt, from_table, extra_froms, from_hints, **kw):
        pass


class PromQLDialect(default.DefaultDialect):  # type: ignore
    name = "bi_promql"
    statement_compiler = PromQLCompiler

    @classmethod
    def dbapi(cls):
        from bi_sqlalchemy_promql import dbapi  # noqa

        return dbapi

    def get_columns(self, connection, table_name, schema=None, **kw):
        pass

    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        pass

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        pass

    def get_table_names(self, connection, schema=None, **kw):
        pass

    def get_temp_table_names(self, connection, schema=None, **kw):
        pass

    def get_view_names(self, connection, schema=None, **kw):
        pass

    def get_temp_view_names(self, connection, schema=None, **kw):
        pass

    def get_view_definition(self, connection, view_name, schema=None, **kw):
        pass

    def get_indexes(self, connection, table_name, schema=None, **kw):
        pass

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        pass

    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        pass

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        pass

    def has_table(self, connection, table_name, schema=None, **kw):
        pass

    def has_sequence(self, connection, sequence_name, schema=None, *kw):
        pass

    def _get_server_version_info(self, connection):
        pass

    def _get_default_schema_name(self, connection):
        pass

    def do_begin_twophase(self, connection, xid):
        pass

    def do_prepare_twophase(self, connection, xid):
        pass

    def do_rollback_twophase(self, connection, xid, is_prepared=True, recover=False):
        pass

    def do_commit_twophase(self, connection, xid, is_prepared=True, recover=False):
        pass

    def do_recover_twophase(self, connection):
        pass

    def set_isolation_level(self, dbapi_conn, level):
        pass

    def get_isolation_level(self, dbapi_conn):
        pass

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        pass

    def get_sequence_names(self, connection, schema=None, **kw):
        pass

    def do_set_input_sizes(self, cursor, list_of_tuples, context):
        pass


dialect = PromQLDialect


def register_dialect():
    return registry.register("bi_promql", "bi_sqlalchemy_promql.base", "PromQLDialect")
