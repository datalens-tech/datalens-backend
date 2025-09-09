from typing import Optional

import pymysql
import sqlalchemy

import dl_core.connectors.base.error_transformer as error_transformer
from dl_core.connectors.base.error_transformer import (
    ExcMatchCondition,
    wrapper_exc_is_and_matches_re,
)
from dl_core.connectors.base.error_transformer import DbErrorTransformer
from dl_core.connectors.base.error_transformer import ErrorTransformerRule as Rule
import dl_core.exc as exc

from dl_connector_mysql.core.exc import MysqlSourceDoesNotExistError


TABLE_DOES_NOT_EXIST_ERROR_CODE = 1146
SOURCE_CONNECT_ERROR_CODE = 2003
SQL_SYNTAX_ERROR_CODE = 1064


def is_table_does_not_exist_async_error() -> ExcMatchCondition:
    def _(exc: Exception) -> bool:
        if isinstance(exc, pymysql.ProgrammingError):
            if len(exc.args) >= 2 and exc.args[0] == TABLE_DOES_NOT_EXIST_ERROR_CODE:
                return True
        return False

    return _


def is_table_does_not_exist_sync_error() -> ExcMatchCondition:
    def _(exc: Exception) -> bool:
        if isinstance(exc, sqlalchemy.exc.ProgrammingError):
            orig = getattr(exc, "orig", None)
            if orig and len(orig.args) >= 2 and orig.args[0] == TABLE_DOES_NOT_EXIST_ERROR_CODE:
                return True
        return False

    return _


def is_source_connect_async_error() -> ExcMatchCondition:
    def _(exc: Exception) -> bool:
        if isinstance(exc, pymysql.OperationalError):
            if len(exc.args) >= 2 and exc.args[0] == SOURCE_CONNECT_ERROR_CODE:
                return True
        return False

    return _


def is_sql_syntax_error_async_error() -> ExcMatchCondition:
    def _(exc: Exception) -> bool:
        if isinstance(exc, pymysql.ProgrammingError):
            if len(exc.args) >= 2 and exc.args[0] == SQL_SYNTAX_ERROR_CODE:
                return True
        return False

    return _


def is_sql_syntax_error_sync_error() -> ExcMatchCondition:
    def _(exc: Exception) -> bool:
        if isinstance(exc, sqlalchemy.exc.ProgrammingError):
            orig = getattr(exc, "orig", None)
            if orig and len(orig.args) >= 2 and orig.args[0] == SQL_SYNTAX_ERROR_CODE:
                return True
        return False

    return _


class AsyncMysqlChainedDbErrorTransformer(error_transformer.ChainedDbErrorTransformer):
    @staticmethod
    def _get_error_kw(
        debug_compiled_query: Optional[str], orig_exc: Optional[Exception], wrapper_exc: Exception
    ) -> error_transformer.DBExcKWArgs:
        if isinstance(wrapper_exc, pymysql.ProgrammingError):
            return dict(
                db_message=str(orig_exc) if orig_exc else str(wrapper_exc),
                query=debug_compiled_query,
                orig=wrapper_exc,  # keep orig exception for .args
                details={},
            )
        return error_transformer.ChainedDbErrorTransformer._get_error_kw(debug_compiled_query, orig_exc, wrapper_exc)


async_mysql_db_error_transformer: DbErrorTransformer = AsyncMysqlChainedDbErrorTransformer(
    (
        Rule(
            when=is_source_connect_async_error(),
            then_raise=exc.SourceConnectError,
        ),
        Rule(
            when=is_table_does_not_exist_async_error(),
            then_raise=MysqlSourceDoesNotExistError,
        ),
        Rule(
            when=wrapper_exc_is_and_matches_re(
                wrapper_exc_cls=pymysql.ProgrammingError,
                err_regex_str="You have an error in your SQL syntax",
            ),
            then_raise=exc.InvalidQuery,
        ),
        Rule(
            when=is_sql_syntax_error_async_error(),
            then_raise=exc.InvalidQuery,
        ),
        Rule(
            when=wrapper_exc_is_and_matches_re(
                wrapper_exc_cls=RuntimeError,
                err_regex_str=".*Received LOAD_LOCAL.*",
            ),
            then_raise=exc.SourceProtocolError,
        ),
    )
    + error_transformer.default_error_transformer_rules
)


sync_mysql_db_error_transformer: DbErrorTransformer = error_transformer.make_default_transformer_with_custom_rules(
    Rule(
        when=is_table_does_not_exist_sync_error(),
        then_raise=MysqlSourceDoesNotExistError,
    ),
    Rule(
        when=is_sql_syntax_error_sync_error(),
        then_raise=exc.InvalidQuery,
    ),
    Rule(
        when=wrapper_exc_is_and_matches_re(
            wrapper_exc_cls=RuntimeError,
            err_regex_str=".*Received LOAD_LOCAL.*",
        ),
        then_raise=exc.SourceProtocolError,
    ),
)
