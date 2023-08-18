from __future__ import annotations

from aiohttp import web

from bi_api_commons.aiohttp.aiohttp_wrappers import DLRequestView


class PingView(DLRequestView):
    async def get(self):  # type: ignore  # TODO: fix
        return web.json_response({
            'request_id': self.dl_request.rci.request_id,
        })


class PingDbView(web.View):
    async def get(self):  # type: ignore  # TODO: fix
        async with self.request.app.db.acquire() as conn:  # type: ignore  # TODO: fix
            data = await conn.scalar('select 1')
            if data == 1:
                return web.json_response({
                    'db_status': 'ok',
                })
            else:
                raise web.HTTPBadRequest()
