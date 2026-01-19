import aiohttp.web
import attr

import dl_app_api_base.request_context as request_context
import dl_constants


@attr.define(frozen=True, kw_only=True)
class RequestContextMiddleware:
    _context_manager: request_context.DefaultRequestContextManager

    @aiohttp.web.middleware
    async def process(
        self,
        request: aiohttp.web.Request,
        handler: aiohttp.typedefs.Handler,
    ) -> aiohttp.web.StreamResponse:
        request_context = self._context_manager.create(aiohttp_request=request)
        try:
            response = await handler(request)
            response.headers[dl_constants.DLHeadersCommon.REQUEST_ID.value] = request_context.get_request_id()
            return response
        finally:
            self._context_manager.clear()
