from __future__ import annotations

import json

import dl_core.exc as exc
import dl_core.connectors.base.error_transformer as error_transformer
from dl_core.connectors.base.error_transformer import (
    orig_exc_is,
    DbErrorTransformer,
    ErrorTransformerRule as Rule,
)


bitrix_error_transformer: DbErrorTransformer = error_transformer.make_default_transformer_with_custom_rules(
    Rule(
        when=orig_exc_is(orig_exc_cls=json.JSONDecodeError),
        then_raise=exc.SourceResponseError,
    )
)
