from __future__ import annotations

from typing import ClassVar

from aiohttp import web

from dl_api_commons.aiohttp.aiohttp_wrappers import (
    DLRequestView,
    RequiredResource,
    RequiredResourceCommon,
)


class PingView(DLRequestView):
    REQUIRED_RESOURCES: ClassVar[frozenset[RequiredResource]] = frozenset({RequiredResourceCommon.SKIP_AUTH})

    async def get(self) -> web.StreamResponse:
        return web.json_response(
            {
                "request_id": self.dl_request.rci.request_id,
            }
        )
