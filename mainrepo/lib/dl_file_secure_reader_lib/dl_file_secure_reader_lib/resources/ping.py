from __future__ import annotations

from aiohttp import web


class PingView(web.View):
    async def get(self) -> web.Response:
        return web.Response()
