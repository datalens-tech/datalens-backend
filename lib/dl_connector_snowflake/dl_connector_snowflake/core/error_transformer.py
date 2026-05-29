import sqlalchemy

from dl_core.connectors.base.error_transformer import (
    ChainedDbErrorTransformer,
    DbErrorTransformer,
)
from dl_core.connectors.base.error_transformer import ErrorTransformerRule as Rule
from dl_core.connectors.base.error_transformer import ExcMatchCondition

from dl_connector_snowflake.core.exc import SnowflakeAccessTokenError


def is_access_token_error() -> ExcMatchCondition:
    def func(exc: Exception) -> bool:
        if isinstance(exc, sqlalchemy.exc.DatabaseError):
            orig = getattr(exc, "orig", None)
            if orig and (getattr(orig, "errno", None) == 250001):
                return True

        return False

    return func


snowflake_error_transformer: DbErrorTransformer = ChainedDbErrorTransformer(
    [Rule(when=is_access_token_error(), then_raise=SnowflakeAccessTokenError)]
)
