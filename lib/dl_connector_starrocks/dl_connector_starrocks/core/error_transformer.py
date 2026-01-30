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

from dl_connector_starrocks.core.exc import StarRocksSourceDoesNotExistError


# StarRocks error codes
TABLE_DOES_NOT_EXIST_ERROR_CODE = 1051  # "Unknown table"
SQL_SYNTAX_ERROR_CODE = 1064


def is_table_does_not_exist_async_error() -> ExcMatchCondition:
    def _(exc: Exception) -> bool:
        if isinstance(exc, pymysql.OperationalError):
            if len(exc.args) >= 2 and exc.args[0] == TABLE_DOES_NOT_EXIST_ERROR_CODE:
                return True
        return False

    return _


def is_table_does_not_exist_sync_error() -> ExcMatchCondition:
    def _(exc: Exception) -> bool:
        if isinstance(exc, sqlalchemy.exc.OperationalError):
            orig = getattr(exc, "orig", None)
            if orig and len(orig.args) >= 2 and orig.args[0] == TABLE_DOES_NOT_EXIST_ERROR_CODE:
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


class AsyncStarRocksChainedDbErrorTransformer(error_transformer.ChainedDbErrorTransformer):
    @staticmethod
    def _get_error_kw(
        debug_query: str | None,
        orig_exc: Exception | None,
        wrapper_exc: Exception,
        inspector_query: str | None,
    ) -> error_transformer.DBExcKWArgs:
        if isinstance(wrapper_exc, (pymysql.OperationalError, pymysql.ProgrammingError)):
            return dict(
                db_message=str(orig_exc) if orig_exc else str(wrapper_exc),
                query=debug_query,
                inspector_query=inspector_query,
                orig=wrapper_exc,  # keep orig exception for .args
                details={},
            )
        return error_transformer.ChainedDbErrorTransformer._get_error_kw(
            debug_query, orig_exc, wrapper_exc, inspector_query
        )


async_starrocks_db_error_transformer: DbErrorTransformer = AsyncStarRocksChainedDbErrorTransformer(
    (
        Rule(
            when=is_table_does_not_exist_async_error(),
            then_raise=StarRocksSourceDoesNotExistError,
        ),
        Rule(
            when=wrapper_exc_is_and_matches_re(
                wrapper_exc_cls=pymysql.ProgrammingError,
                err_regex_str=".*SQL syntax.*",
            ),
            then_raise=exc.InvalidQuery,
        ),
        Rule(
            when=is_sql_syntax_error_async_error(),
            then_raise=exc.InvalidQuery,
        ),
    )
    + error_transformer.default_error_transformer_rules
)


sync_starrocks_db_error_transformer: DbErrorTransformer = error_transformer.make_default_transformer_with_custom_rules(
    Rule(
        when=is_table_does_not_exist_sync_error(),
        then_raise=StarRocksSourceDoesNotExistError,
    ),
    Rule(
        when=is_sql_syntax_error_sync_error(),
        then_raise=exc.InvalidQuery,
    ),
)
