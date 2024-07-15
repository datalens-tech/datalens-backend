import logging
import typing

import aiohttp.web as aiohttp_web
import attr

import dl_rate_limiter.request_rate_limiter as request_rate_limiter


logger = logging.getLogger(__name__)


AioHTTPHandler = typing.Callable[[aiohttp_web.Request], typing.Awaitable[aiohttp_web.Response]]


@attr.s(auto_attribs=True)
class AioHTTPMiddleware:
    _rate_limiter: request_rate_limiter.AsyncRequestRateLimiterProtocol

    async def _process(self, request: aiohttp_web.Request, handler: AioHTTPHandler) -> aiohttp_web.Response:
        rate_limiter_request = request_rate_limiter.Request(
            url=str(request.path),
            method=request.method,
            headers=request.headers,
        )

        result = await self._rate_limiter.check_limit(request=rate_limiter_request)

        if result:
            logger.info("No request limit was found")
            return await handler(request)

        logger.info("Request was rate limited")
        return aiohttp_web.json_response(
            status=429,
            data={"description": "Too Many Requests"},
        )

    @aiohttp_web.middleware
    async def process(self, request: aiohttp_web.Request, handler: AioHTTPHandler) -> aiohttp_web.Response:
        try:
            return await self._process(request, handler)
        except Exception as exc:
            logger.info("Failed to check request limit", exc_info=exc)

        return await handler(request)


__all__ = [
    "AioHTTPMiddleware",
]
