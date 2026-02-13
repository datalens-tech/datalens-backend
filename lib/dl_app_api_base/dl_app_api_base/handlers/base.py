import abc
import http
import logging
from typing import (
    Any,
    ClassVar,
)

import aiohttp.web
import attrs
from typing_extensions import Self

import dl_json
import dl_pydantic


LOGGER = logging.getLogger(__name__)


class BaseSchema(dl_pydantic.BaseSchema):
    ...


class BaseResponseSchema(BaseSchema):
    ...


class HttpError(aiohttp.web.HTTPError):
    def __init__(
        self,
        status: int,
        text: str,
    ):
        self.status_code = status
        super().__init__(text=text)


class ErrorResponseSchema(BaseResponseSchema):
    message: str
    code: str

    def http_error(self, status: int) -> HttpError:
        json_data = self.model_dump(mode="json")
        raw_data = dl_json.dumps_str(json_data)
        return HttpError(text=raw_data, status=status)


class BadRequestResponseSchema(ErrorResponseSchema):
    message: str = "Bad request"
    code: str = "ERR.API.BAD_REQUEST"


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
            LOGGER.exception(f"Bad request: {request.text}")
            response = BadRequestResponseSchema()
            raise response.http_error(status=http.HTTPStatus.BAD_REQUEST)


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
