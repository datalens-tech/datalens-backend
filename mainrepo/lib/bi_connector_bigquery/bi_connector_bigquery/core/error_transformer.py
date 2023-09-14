import google.api_core.exceptions as ga_exc

from bi_core.connectors.base.error_transformer import (
    ChainedDbErrorTransformer,
    DbErrorTransformer,
)
from bi_core.connectors.base.error_transformer import ErrorTransformerRule as Rule
from bi_core.connectors.base.error_transformer import wrapper_exc_is
import bi_core.exc as exc

big_query_db_error_transformer: DbErrorTransformer = ChainedDbErrorTransformer(
    [Rule(when=wrapper_exc_is(wrapper_exc_cls=ga_exc.Forbidden), then_raise=exc.UserQueryAccessDenied)]
)
