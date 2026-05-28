from __future__ import annotations

import json

import dl_core.connectors.base.error_transformer as error_transformer
from dl_core.connectors.base.error_transformer import DbErrorTransformer, orig_exc_is
from dl_core.connectors.base.error_transformer import ErrorTransformerRule as Rule
import dl_core.exc as exc

bitrix_error_transformer: DbErrorTransformer = error_transformer.make_default_transformer_with_custom_rules(
    Rule(
        when=orig_exc_is(orig_exc_cls=json.JSONDecodeError),
        then_raise=exc.SourceResponseError,
    )
)
