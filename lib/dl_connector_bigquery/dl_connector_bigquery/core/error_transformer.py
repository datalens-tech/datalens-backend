import google.api_core.exceptions as ga_exc

from dl_core.connectors.base.error_transformer import (
    ChainedDbErrorTransformer,
    DbErrorTransformer,
    wrapper_exc_is,
)
from dl_core.connectors.base.error_transformer import ErrorTransformerRule as Rule
import dl_core.exc as exc

big_query_db_error_transformer: DbErrorTransformer = ChainedDbErrorTransformer(
    [Rule(when=wrapper_exc_is(wrapper_exc_cls=ga_exc.Forbidden), then_raise=exc.UserQueryAccessDenied)]
)
