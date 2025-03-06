from enum import Enum
from typing import Optional

import sqlalchemy
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

from dl_connector_trino.core.exc import (
    TrinoCatalogDoesNotExistError,
    TrinoSchemaDoesNotExistError,
    TrinoTableDoesNotExistError,
)


class TrinoSourceDoesNotExistErrorType(str, Enum):
    TABLE_NOT_FOUND = "TABLE_NOT_FOUND"
    SCHEMA_NOT_FOUND = "SCHEMA_NOT_FOUND"
    CATALOG_NOT_FOUND = "CATALOG_NOT_FOUND"


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
    if not isinstance(exc, sqlalchemy.exc.ProgrammingError):
        return None

    orig = getattr(exc, "orig", None)
    if isinstance(orig, TrinoUserError):
        return orig

    return None


def is_trino_table_does_not_exist_error() -> ExcMatchCondition:
    def _(exc: Exception) -> bool:
        orig = trino_user_error_or_none(exc)
        if orig is None:
            return False

        return orig.error_name == TrinoSourceDoesNotExistErrorType.TABLE_NOT_FOUND

    return _


def is_trino_schema_does_not_exist_error() -> ExcMatchCondition:
    def _(exc: Exception) -> bool:
        orig = trino_user_error_or_none(exc)
        if orig is None:
            return False

        return orig.error_name == TrinoSourceDoesNotExistErrorType.SCHEMA_NOT_FOUND

    return _


def is_trino_catalog_does_not_exist_error() -> ExcMatchCondition:
    def _(exc: Exception) -> bool:
        orig = trino_user_error_or_none(exc)
        if orig is None:
            return False

        return orig.error_name == TrinoSourceDoesNotExistErrorType.CATALOG_NOT_FOUND

    return _


trino_error_transformer = TrinoErrorTransformer(
    rule_chain=(
        Rule(
            when=is_trino_table_does_not_exist_error(),
            then_raise=TrinoTableDoesNotExistError,
        ),
        Rule(
            when=is_trino_schema_does_not_exist_error(),
            then_raise=TrinoSchemaDoesNotExistError,
        ),
        Rule(
            when=is_trino_catalog_does_not_exist_error(),
            then_raise=TrinoCatalogDoesNotExistError,
        ),
    )
    + default_error_transformer_rules
)
