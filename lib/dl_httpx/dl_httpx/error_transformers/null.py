from typing import Final

import attrs

import dl_httpx.error_transformers.base as base
import dl_httpx.exceptions as exceptions


@attrs.define(frozen=True)
class NullErrorTransformer:
    def transform(self, exception: exceptions.HttpStatusHttpxClientException) -> Exception | None:
        return None


NULL_ERROR_TRANSFORMER: Final[base.ErrorTransformerProtocol] = NullErrorTransformer()
