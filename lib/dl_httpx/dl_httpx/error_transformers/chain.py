from collections.abc import Sequence

import attrs

import dl_httpx.error_transformers.base as base
import dl_httpx.exceptions as exceptions


@attrs.define(frozen=True)
class ChainTransformer:
    transformers: Sequence[base.ErrorTransformerProtocol]

    def transform(self, exception: exceptions.HttpStatusHttpxClientError) -> Exception | None:
        for transformer in self.transformers:
            result = transformer.transform(exception)
            if result is not None:
                return result
        return None
