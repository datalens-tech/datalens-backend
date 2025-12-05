import logging
import time

import aiohttp.typedefs
import aiohttp.web
import attr


LOGGER = logging.getLogger(__name__)


def _request_to_string(request: aiohttp.web.Request) -> str:
    return f"Request(method={request.method}, path={request.path})"


def _response_to_string(response: aiohttp.web.StreamResponse) -> str:
    return f"Response(status={response.status}, reason={response.reason})"


def _elapsed_time_to_string(start_time: float) -> str:
    elapsed_seconds = time.time() - start_time

    if elapsed_seconds < 1:
        return f"{elapsed_seconds * 1000:.3f} ms"
    else:
        return f"{elapsed_seconds:.3f} s"


@attr.define(frozen=True, kw_only=True)
class LoggingMiddleware:
    @aiohttp.web.middleware
    async def process(
        self,
        request: aiohttp.web.Request,
        handler: aiohttp.typedefs.Handler,
    ) -> aiohttp.web.StreamResponse:
        request_start_time = time.time()
        LOGGER.info("Server Received: %s", _request_to_string(request))

        try:
            response = await handler(request)
        except Exception as exc:
            LOGGER.exception(
                "Server Failed to process %s, elapsed time: %s",
                _request_to_string(request),
                _elapsed_time_to_string(request_start_time),
            )
            raise exc
        else:
            LOGGER.info(
                "Server Answered: %s for %s, elapsed time: %s",
                _response_to_string(response),
                _request_to_string(request),
                _elapsed_time_to_string(request_start_time),
            )

        return response
