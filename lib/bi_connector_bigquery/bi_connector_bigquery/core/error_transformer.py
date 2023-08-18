import google.api_core.exceptions as ga_exc

import bi_core.exc as exc
from bi_core.connectors.base.error_transformer import wrapper_exc_is, ChainedDbErrorTransformer, DbErrorTransformer, \
    ErrorTransformerRule as Rule

big_query_db_error_transformer: DbErrorTransformer = ChainedDbErrorTransformer([
    Rule(
        when=wrapper_exc_is(wrapper_exc_cls=ga_exc.Forbidden),
        then_raise=exc.UserQueryAccessDenied
    )
])
