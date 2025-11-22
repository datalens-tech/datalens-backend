import http
import typing

import aiohttp.typedefs as aiohttp_typedefs
import aiohttp.web as aiohttp_web
from typing_extensions import Self

import dl_json


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
        data: typing.Any,
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


__all__ = [
    "Response",
]
