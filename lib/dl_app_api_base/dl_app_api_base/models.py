from typing import Any

import aiohttp.web
from typing_extensions import Self

import dl_json
import dl_pydantic


class BaseSchema(dl_pydantic.BaseModel):
    ...


class BaseRequestSchema(BaseSchema):
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
