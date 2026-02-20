import http
import logging
from typing import (
    Mapping,
    Protocol,
)

import aiohttp.typedefs
import aiohttp.web
import attr
import pydantic

import dl_app_api_base.handlers


LOGGER = logging.getLogger(__name__)


class NotFoundErrorResponseSchema(dl_app_api_base.handlers.ErrorResponseSchema):
    message: str = "Not found"
    code: str = "NOT_FOUND"
    status_code: pydantic.json_schema.SkipJsonSchema[http.HTTPStatus] = http.HTTPStatus.NOT_FOUND


class InternalServerErrorResponseSchema(dl_app_api_base.handlers.ErrorResponseSchema):
    message: str = "Internal server error"
    code: str = "INTERNAL_SERVER_ERROR"
    status_code: pydantic.json_schema.SkipJsonSchema[http.HTTPStatus] = http.HTTPStatus.INTERNAL_SERVER_ERROR


class ErrorHandlerProtocol(Protocol):
    def __call__(self, exc: Exception) -> aiohttp.web.StreamResponse | None:
        ...


DEFAULT_ERROR_MAP: Mapping[type[Exception], dl_app_api_base.handlers.ErrorResponseSchema] = {
    aiohttp.web.HTTPNotFound: NotFoundErrorResponseSchema(),
}


@attr.define(frozen=True, kw_only=True)
class MapErrorHandler:
    _map: Mapping[type[Exception], dl_app_api_base.handlers.ErrorResponseSchema]

    def process(self, exc: Exception) -> aiohttp.web.StreamResponse | None:
        schema = self._map.get(exc.__class__)
        if schema is None:
            return None

        return schema.as_response()


@attr.define(frozen=True, kw_only=True)
class ErrorHandlingMiddleware:
    error_handlers: list[ErrorHandlerProtocol] = attr.field(factory=list)

    @aiohttp.web.middleware
    async def process(
        self,
        request: aiohttp.web.Request,
        handler: aiohttp.typedefs.Handler,
    ) -> aiohttp.web.StreamResponse:
        try:
            return await handler(request)
        except dl_app_api_base.handlers.ResponseException as e:
            return e
        except Exception as exc:
            return self.handle_error(exc)

    def handle_error(self, exception: Exception) -> aiohttp.web.StreamResponse:
        try:
            return self._handle_error(exception)
        except Exception:
            LOGGER.exception("Failed to handle request error")
            return InternalServerErrorResponseSchema().as_response()

    def _handle_error(self, exc: Exception) -> aiohttp.web.StreamResponse:
        for handler in self.error_handlers:
            response = handler(exc)
            if response is not None:
                return response

        LOGGER.exception("Unhandled request error")
        return InternalServerErrorResponseSchema().as_response()
