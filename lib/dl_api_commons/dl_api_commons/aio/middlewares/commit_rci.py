from aiohttp import web
from aiohttp.typedefs import (
    Handler,
    Middleware,
)

from dl_api_commons.aiohttp import aiohttp_wrappers


def commit_rci_middleware() -> Middleware:
    @web.middleware
    @aiohttp_wrappers.DLRequestBase.use_dl_request
    async def actual_commit_rci_middleware(
        dl_request: aiohttp_wrappers.DLRequestBase, handler: Handler
    ) -> web.StreamResponse:
        dl_request.commit_rci()
        return await handler(dl_request.request)

    return actual_commit_rci_middleware
