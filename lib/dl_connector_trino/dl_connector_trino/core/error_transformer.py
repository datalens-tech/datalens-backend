from typing import Optional

from sqlalchemy import exc as sa_exc
from trino.exceptions import (
    TrinoQueryError,
    TrinoUserError,
)

from dl_core.connectors.base.error_transformer import (
    ChainedDbErrorTransformer,
    DBExcKWArgs,
)
from dl_core.connectors.base.error_transformer import (
    ExcMatchCondition,
    default_error_transformer_rules,
)
from dl_core.connectors.base.error_transformer import ErrorTransformerRule as Rule
import dl_core.exc as exc


TRINO_SOURCE_DOES_NOT_EXIST_ERROR_TYPES = (
    "TABLE_NOT_FOUND",
    "SCHEMA_NOT_FOUND",
    "CATALOG_NOT_FOUND",
)


class ExpressionNotAggregateError(exc.InvalidQuery):
    """
    Raised when Trino receives a SELECT statement containing multiple identical parameterized expressions with the same parameter value.
    In this case, the query should be compiled before being sent to Trino.
    """


class TrinoErrorTransformer(ChainedDbErrorTransformer):
    @staticmethod
    def _get_error_kw(
        debug_compiled_query: Optional[str], orig_exc: Optional[Exception], wrapper_exc: Exception
    ) -> DBExcKWArgs:
        if isinstance(orig_exc, TrinoQueryError):
            return dict(
                db_message=orig_exc.message,
                query=debug_compiled_query,
                orig=orig_exc,
                details={
                    "error_name": orig_exc.error_name,
                    "error_type": orig_exc.error_type,
                    "error_code": orig_exc.error_code,
                    "query_id": orig_exc.query_id,
                },
            )

        return ChainedDbErrorTransformer._get_error_kw(debug_compiled_query, orig_exc, wrapper_exc)


def trino_user_error_or_none(exc: Exception) -> Optional[TrinoUserError]:
    if not isinstance(exc, sa_exc.ProgrammingError):
        return None

    orig = getattr(exc, "orig", None)
    if isinstance(orig, TrinoUserError):
        return orig

    return None


def trino_query_error_or_none(exc: Exception) -> Optional[TrinoQueryError]:
    if not isinstance(exc, sa_exc.DBAPIError):
        return None

    orig = getattr(exc, "orig", None)
    if isinstance(orig, TrinoQueryError):
        return orig

    return None


def is_trino_column_does_not_exist_error() -> ExcMatchCondition:
    def _(exc: Exception) -> bool:
        orig = trino_user_error_or_none(exc)
        if orig is None:
            return False

        return orig.error_name == "COLUMN_NOT_FOUND"

    return _


def is_trino_source_does_not_exist_error() -> ExcMatchCondition:
    def _(exc: Exception) -> bool:
        orig = trino_user_error_or_none(exc)
        if orig is None:
            return False

        return orig.error_name in TRINO_SOURCE_DOES_NOT_EXIST_ERROR_TYPES

    return _


def is_trino_syntax_error() -> ExcMatchCondition:
    def _(exc: Exception) -> bool:
        orig = trino_user_error_or_none(exc)
        if orig is None:
            return False

        return orig.error_name == "SYNTAX_ERROR"

    return _


def is_trino_expression_not_aggregate_error() -> ExcMatchCondition:
    def _(exc: Exception) -> bool:
        orig = trino_user_error_or_none(exc)
        if orig is None:
            return False

        return orig.error_name == "EXPRESSION_NOT_AGGREGATE"

    return _


def is_trino_out_of_memory_error() -> ExcMatchCondition:
    def _(exc: Exception) -> bool:
        orig = trino_query_error_or_none(exc)
        if orig is None:
            return False

        return orig.error_type == "INSUFFICIENT_RESOURCES"

    return _


def is_trino_fallback_error() -> ExcMatchCondition:
    def _(exc: Exception) -> bool:
        return isinstance(exc, sa_exc.DBAPIError)

    return _


trino_error_transformer = TrinoErrorTransformer(
    rule_chain=(
        Rule(
            when=is_trino_column_does_not_exist_error(),
            then_raise=exc.ColumnDoesNotExist,
        ),
        Rule(
            when=is_trino_source_does_not_exist_error(),
            then_raise=exc.SourceDoesNotExist,
        ),
        Rule(
            when=is_trino_syntax_error(),
            then_raise=exc.InvalidQuery,
        ),
        Rule(
            when=is_trino_out_of_memory_error(),
            then_raise=exc.DbMemoryLimitExceeded,
        ),
        Rule(
            when=is_trino_expression_not_aggregate_error(),
            then_raise=ExpressionNotAggregateError,
        ),
        Rule(
            when=is_trino_fallback_error(),
            then_raise=exc.DatabaseQueryError,
        ),
    )
    + default_error_transformer_rules
)
