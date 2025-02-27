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

from dl_connector_trino.core.exc import TrinoSourceDoesNotExistError


class TrinoErrorTransformer(ChainedDbErrorTransformer):
    @staticmethod
    def _get_error_kw(
        debug_compiled_query: Optional[str], orig_exc: Optional[Exception], wrapper_exc: Exception
    ) -> DBExcKWArgs:
        assert isinstance(orig_exc, TrinoQueryError)
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


def is_table_does_not_exist_sync_error() -> ExcMatchCondition:
    def _(exc: Exception) -> bool:
        if isinstance(exc, sqlalchemy.exc.ProgrammingError):
            orig = getattr(exc, "orig", None)
            if not isinstance(orig, TrinoUserError):
                return False

            if orig.error_name == "TABLE_NOT_FOUND":
                return True
        return False

    return _


trino_error_transformer = TrinoErrorTransformer(
    rule_chain=(
        Rule(
            when=is_table_does_not_exist_sync_error(),
            then_raise=TrinoSourceDoesNotExistError,
        ),
    )
    + default_error_transformer_rules
)
