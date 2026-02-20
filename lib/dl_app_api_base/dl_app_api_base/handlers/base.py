import abc
import http
import logging
from typing import (
    Any,
    ClassVar,
)

import aiohttp.typedefs as aiohttp_typedefs
import aiohttp.web
import aiohttp.web as aiohttp_web
import attrs
import pydantic
from typing_extensions import Self

import dl_json
import dl_pydantic


LOGGER = logging.getLogger(__name__)


class BaseSchema(dl_pydantic.BaseSchema):
    ...


class Response(aiohttp_web.Response):
    @classmethod
    def with_bytes(
        cls,
        body: bytes,
        status: int = http.HTTPStatus.OK,
        reason: str | None = None,
        headers: aiohttp_typedefs.LooseHeaders | None = None,
        content_type: str | None = None,
    ) -> Self:
        return cls(
            body=body,
            status=status,
            reason=reason,
            headers=headers,
            content_type=content_type,
        )

    @classmethod
    def with_data(
        cls,
        data: Any,
        status: int = http.HTTPStatus.OK,
        reason: str | None = None,
        headers: aiohttp_typedefs.LooseHeaders | None = None,
    ) -> Self:
        body = dl_json.dumps_bytes(data)
        return cls.with_bytes(
            body=body,
            status=status,
            reason=reason,
            headers=headers,
            content_type="application/json",
        )

    @classmethod
    def with_model(
        cls,
        schema: BaseSchema,
        status: int = http.HTTPStatus.OK,
        reason: str | None = None,
        headers: aiohttp_typedefs.LooseHeaders | None = None,
    ) -> Self:
        return cls.with_data(
            data=schema.model_dump(mode="json"),
            status=status,
            reason=reason,
            headers=headers,
        )

    @classmethod
    def with_status(
        cls,
        status: int = http.HTTPStatus.OK,
        reason: str | None = None,
        headers: aiohttp_typedefs.LooseHeaders | None = None,
    ) -> Self:
        return cls(
            status=status,
            reason=reason,
            headers=headers,
        )


class ResponseException(Response, Exception):
    ...


class BaseResponseSchema(BaseSchema):
    ...


class ErrorResponseSchema(BaseResponseSchema):
    """
    Base response schema for error responses.
    Use as_response() to return response from handlers and middlewares
    Use as_exception() to return response only from places where
    return statement is not possible without obstructing original return value (like checkers in middlewares)

    Example:
    >>> return ErrorResponseSchema().as_response()
    >>> raise ErrorResponseSchema().as_exception()
    """

    message: str
    code: str
    status_code: pydantic.json_schema.SkipJsonSchema[http.HTTPStatus]

    def as_data(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"status_code"})

    def as_response(self) -> Response:
        return Response.with_data(data=self.as_data(), status=self.status_code)

    def as_exception(self) -> ResponseException:
        return ResponseException.with_data(data=self.as_data(), status=self.status_code)


class BadRequestResponseSchema(ErrorResponseSchema):
    message: str = "Bad request"
    code: str = "ERR.API.BAD_REQUEST"
    status_code: pydantic.json_schema.SkipJsonSchema[http.HTTPStatus] = http.HTTPStatus.BAD_REQUEST


class BaseRequestSchema(BaseSchema):
    path: BaseSchema
    query: BaseSchema
    body: BaseSchema

    @classmethod
    async def _get_body(cls, request: aiohttp.web.Request) -> dict[str, Any] | None:
        if not request.body_exists:
            return None

        raw_body = await request.read()
        return dl_json.loads_bytes(raw_body)

    @classmethod
    async def from_request(cls, request: aiohttp.web.Request) -> Self:
        body = await cls._get_body(request)
        body = body or {}

        return cls.model_validate(
            {
                "path": request.match_info,
                "query": request.query,
                "body": body,
            }
        )

    @classmethod
    async def try_from_request(cls, request: aiohttp.web.Request) -> Self:
        try:
            return await cls.from_request(request)
        except ValueError:
            text = await request.text()
            LOGGER.exception(f"Bad request: {text}")
            raise BadRequestResponseSchema().as_exception()


class BaseHandler(abc.ABC):
    class RequestSchema(BaseRequestSchema):
        ...

    class ResponseSchema(BaseResponseSchema):
        ...

    OPENAPI_TAGS: ClassVar[list[str]] = []
    OPENAPI_DESCRIPTION: ClassVar[str] = ""
    OPENAPI_INCLUDE: ClassVar[bool] = True

    @property
    def _response_schemas(self) -> dict[http.HTTPStatus, type[BaseResponseSchema]]:
        return {
            http.HTTPStatus.OK: self.ResponseSchema,
            http.HTTPStatus.BAD_REQUEST: BadRequestResponseSchema,
        }

    @property
    def _request_schema(self) -> type[BaseRequestSchema]:
        return self.RequestSchema

    @abc.abstractmethod
    async def process(self, request: aiohttp.web.Request) -> aiohttp.web.StreamResponse:
        ...


@attrs.define(frozen=True, kw_only=True)
class Route:
    path: str
    method: str
    handler: BaseHandler
