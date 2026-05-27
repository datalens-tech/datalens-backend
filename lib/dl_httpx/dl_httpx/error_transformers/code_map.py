import json
import logging

import attrs

import dl_httpx.error_transformers.base as base
import dl_httpx.exceptions as exceptions

LOGGER = logging.getLogger(__name__)


@attrs.define(frozen=True)
class CodeMapTransformer:
    code_map: dict[str, base.ExceptionFactoryProtocol]
    status_body_path: tuple[str, ...] = ("code",)

    def transform(self, exception: exceptions.HttpStatusHttpxClientException) -> Exception | None:
        try:
            body = exception.response.json()
        except json.JSONDecodeError:
            LOGGER.debug("CodeMapTransformer: response body is not valid JSON; passing through")
            return None

        value: object = body
        for key in self.status_body_path:
            if not isinstance(value, dict):
                LOGGER.debug(
                    "CodeMapTransformer: response body cannot be navigated to %r; passing through",
                    self.status_body_path,
                )
                return None
            value = value.get(key)
            if value is None:
                LOGGER.debug(
                    "CodeMapTransformer: response body has no value at %r; passing through",
                    self.status_body_path,
                )
                return None

        if not isinstance(value, str):
            LOGGER.debug(
                "CodeMapTransformer: value at %r is not a string code; passing through",
                self.status_body_path,
            )
            return None

        factory = self.code_map.get(value)
        if factory is None:
            return None

        return factory(exception)
