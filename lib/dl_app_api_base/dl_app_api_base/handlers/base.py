import abc
import http
from typing import (
    Any,
    ClassVar,
)

import aiohttp.web
import attrs
from typing_extensions import Self

import dl_json
import dl_pydantic


class BaseSchema(dl_pydantic.BaseModel):
    ...


class BaseRequestSchema(BaseSchema):
    class MatchInfo(BaseSchema):
        ...

    class Params(BaseSchema):
        ...

    class Body(BaseSchema):
        ...

    @classmethod
    async def _get_body(cls, request: aiohttp.web.Request) -> dict[str, Any] | None:
        if not request.body_exists:
            return None

        raw_body = await request.read()
        return dl_json.loads_bytes(raw_body)

    @classmethod
    async def from_request(cls, request: aiohttp.web.Request) -> Self:
        return cls.model_validate(
            {
                "match_info": request.match_info,
                "query": request.query,
                "body": await cls._get_body(request),
            }
        )


class BaseResponseSchema(BaseSchema):
    ...


class BaseHandler(abc.ABC):
    class ResponseSchema(BaseResponseSchema):
        ...

    class RequestSchema(BaseRequestSchema):
        ...

    TAGS: ClassVar[list[str]] = []
    DESCRIPTION: ClassVar[str] = ""

    @property
    def _response_schemas(self) -> dict[http.HTTPStatus, type[BaseResponseSchema]]:
        return {
            http.HTTPStatus.OK: self.ResponseSchema,
        }

    @abc.abstractmethod
    async def process(self, request: aiohttp.web.Request) -> aiohttp.web.StreamResponse:
        ...


@attrs.define(frozen=True, kw_only=True)
class Route:
    path: str
    method: str
    handler: BaseHandler
