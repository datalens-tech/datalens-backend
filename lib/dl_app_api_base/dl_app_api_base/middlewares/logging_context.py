import logging

import aiohttp.typedefs
import aiohttp.web
import attr

import dl_app_api_base.headers as headers
import dl_app_api_base.request_context as request_context
import dl_logging


LOGGER = logging.getLogger(__name__)


@attr.define(frozen=True, kw_only=True)
class LoggingContextMiddleware:
    _request_context_provider: request_context.RequestContextProviderProtocol[headers.HeadersRequestContextMixin]

    @aiohttp.web.middleware
    async def process(
        self,
        request: aiohttp.web.Request,
        handler: aiohttp.typedefs.Handler,
    ) -> aiohttp.web.StreamResponse:
        request_context = self._request_context_provider.get()
        request_id = request_context.get_request_id()

        initial_log_context = dl_logging.get_log_context()

        if len(initial_log_context) > 0:
            LOGGER.error(
                "Initial log context for request %s is not empty: %s",
                request_id,
                initial_log_context,
            )
            dl_logging.reset_context()

        dl_logging.put_to_context("request_id", request_id)
        dl_logging.put_to_context("trace_id", request_context.get_trace_id())

        try:
            return await handler(request)
        finally:
            dl_logging.reset_context()
