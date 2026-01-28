import aiohttp.web
import attr

import dl_app_api_base.headers as headers
import dl_app_api_base.request_context as request_context
import dl_constants


@attr.define(frozen=True, kw_only=True)
class RequestContextMiddleware:
    _request_context_manager: request_context.RequestContextManagerProtocol[headers.HeadersRequestContextMixin]

    @aiohttp.web.middleware
    async def process(
        self,
        request: aiohttp.web.Request,
        handler: aiohttp.typedefs.Handler,
    ) -> aiohttp.web.StreamResponse:
        request_context = self._request_context_manager.create(aiohttp_request=request)
        try:
            response = await handler(request)
            response.headers[dl_constants.DLHeadersCommon.REQUEST_ID.value] = request_context.get_request_id()
            return response
        finally:
            self._request_context_manager.clear()
