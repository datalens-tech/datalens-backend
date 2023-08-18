from typing import Any, Dict

import datetime
import sqlalchemy as sa

from sqlalchemy.engine import default


class BitrixCompiler(sa.sql.compiler.SQLCompiler):
    def visit_label(
            self,
            label,
            add_to_result_map=None,
            within_label_clause=False,
            within_columns_clause=False,
            render_label_as_label=None,
            **kw,
    ):
        # Labels are not supported at all
        return label.element._compiler_dispatch(
            self,
            within_columns_clause=True,
            within_label_clause=False,
            **kw,
        )

    def render_literal_value(self, value, type_):
        if isinstance(value, (datetime.datetime, datetime.date)):
            return value.isoformat()
        return super().render_literal_value(value, type_)


class BitrixIdentifierPreparer(sa.sql.compiler.IdentifierPreparer):

    def __init__(self, *args, **kwargs):
        quote = "`"
        kwargs = {
            **kwargs,
            'initial_quote': quote,
            'escape_quote': quote,
        }
        super().__init__(*args, **kwargs)


class BitrixDialect(default.DefaultDialect):
    name = "bitrix"
    poolclass = sa.pool.NullPool

    statement_compiler = BitrixCompiler
    ddl_compiler = sa.sql.compiler.DDLCompiler
    type_compiler = sa.sql.compiler.GenericTypeCompiler
    preparer = BitrixIdentifierPreparer

    supports_alter = False
    supports_comments = False
    inline_comments = False

    supports_views = False
    supports_sequences = False
    sequences_optional = False
    preexecute_autoincrement_sequences = False
    postfetch_lastrowid = True
    implicit_returning = False

    supports_right_nested_joins = False
    cte_follows_insert = False

    supports_native_enum = False
    supports_native_boolean = False  # Uncertain
    non_native_boolean_check_constraint = False

    supports_simple_order_by_label = False

    tuple_in_values = False

    supports_native_decimal = False

    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None

    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True

    supports_default_values = False
    supports_empty_insert = False
    supports_multivalues_insert = False

    supports_is_distinct_from = True  # Uncertain

    supports_server_side_cursors = False

    supports_for_update_of = False

    # Clear out:
    ischema_names: Dict[str, Any] = {}

    @classmethod
    def dbapi(cls):
        return None  # Not Applicable... if possible.

    def _check_unicode_returns(self, connection, additional_tests=None):
        return True

    def _check_unicode_description(self, connection):
        return True

    def do_rollback(self, dbapi_connection):
        pass

    def get_columns(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return []

    def _get_default_schema_name(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def _get_server_version_info(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def denormalize_name(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def do_begin_twophase(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def do_commit_twophase(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def do_prepare_twophase(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def do_recover_twophase(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def do_rollback_twophase(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def get_check_constraints(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def get_isolation_level(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def get_primary_keys(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def get_table_comment(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def get_table_names(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def get_temp_table_names(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def get_temp_view_names(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def get_unique_constraints(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def get_view_definition(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def get_view_names(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def has_sequence(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def has_table(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def normalize_name(self, *args, **kwargs):
        raise Exception("Not Implemented")

    def set_isolation_level(self, *args, **kwargs):
        raise Exception("Not Implemented")


dialect = BitrixDialect


def register_dialect(
        name="bi_bitrix",
        module="bi_sqlalchemy_bitrix.base",
        cls="BitrixDialect",
):
    """
    Make sure the dialect is registered
    (normally should happen automagically because of the `entry_point`)
    """
    return sa.dialects.registry.register(name, module, cls)
