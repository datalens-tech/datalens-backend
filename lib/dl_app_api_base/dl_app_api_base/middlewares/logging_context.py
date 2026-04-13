import logging

import aiohttp.typedefs
import aiohttp.web
import attr

import dl_app_api_base.headers as headers
import dl_app_api_base.request_context as request_context
import dl_logging


LOGGER = logging.getLogger(__name__)


@attr.define(frozen=True, kw_only=True)
class StaticLoggingContext:
    app_name: str
    app_version: str

    def put_to_context(self) -> None:
        dl_logging.put_to_context("application.name", self.app_name)
        dl_logging.put_to_context("application.version", self.app_version)


@attr.define(frozen=True, kw_only=True)
class LoggingContextMiddleware:
    _request_context_provider: request_context.RequestContextProviderProtocol[headers.HeadersRequestContextMixin]
    _static_logging_context: StaticLoggingContext

    @aiohttp.web.middleware
    async def process(
        self,
        request: aiohttp.web.Request,
        handler: aiohttp.typedefs.Handler,
    ) -> aiohttp.web.StreamResponse:
        request_context = self._request_context_provider.get()
        request_id = request_context.get_request_id()

        log_context = dl_logging.get_log_context()
        if len(log_context) > 0:
            LOGGER.error(
                "Initial log context for request %s is not empty: %s",
                request_id,
                log_context,
            )
            dl_logging.reset_context()

        self._static_logging_context.put_to_context()
        dl_logging.put_to_context("parent_request_id", request_id)
        dl_logging.put_to_context("trace_id", request_context.get_trace_id())
        dl_logging.put_to_context("user_ip", request_context.get_user_ip())
        dl_logging.put_to_context("request.method", request_context.method)
        dl_logging.put_to_context("request.raw_path", request_context.raw_path)
        dl_logging.put_to_context("request.path_pattern", request_context.path_pattern)
        dl_logging.put_to_context("request.host", request_context.host)

        try:
            return await handler(request)
        finally:
            dl_logging.reset_context()
