import logging
import time

import aiohttp.typedefs
import aiohttp.web
import attr

import dl_app_api_base.request_context as request_context
import dl_app_api_base.request_id as request_id


LOGGER = logging.getLogger(__name__)


@attr.define(frozen=True, kw_only=True)
class LoggingMiddleware:
    _request_context_provider: request_context.RequestContextProviderProtocol[request_id.RequestIdRequestContextMixin]

    def _request_to_string(self, request: aiohttp.web.Request) -> str:
        request_context = self._request_context_provider.get()
        request_id_value = request_context.get_request_id()
        return f"Request(method={request.method}, path={request.path}, request_id={request_id_value})"

    def _response_to_string(self, response: aiohttp.web.StreamResponse) -> str:
        return f"Response(status={response.status}, reason={response.reason})"

    def _elapsed_time_to_string(self, start_time: float) -> str:
        elapsed_seconds = time.time() - start_time

        if elapsed_seconds < 1:
            return f"{elapsed_seconds * 1000:.3f} ms"
        else:
            return f"{elapsed_seconds:.3f} s"

    @aiohttp.web.middleware
    async def process(
        self,
        request: aiohttp.web.Request,
        handler: aiohttp.typedefs.Handler,
    ) -> aiohttp.web.StreamResponse:
        request_start_time = time.time()
        request_string = self._request_to_string(request)
        LOGGER.info("Server Received: %s", request_string)

        try:
            response = await handler(request)
        except Exception as exc:
            LOGGER.exception(
                "Server Failed to process %s, elapsed time: %s",
                request_string,
                self._elapsed_time_to_string(request_start_time),
            )
            raise exc
        else:
            LOGGER.info(
                "Server Answered: %s for %s, elapsed time: %s",
                self._response_to_string(response),
                request_string,
                self._elapsed_time_to_string(request_start_time),
            )

        return response
