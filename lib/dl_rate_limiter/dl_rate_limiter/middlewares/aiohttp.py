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

    @aiohttp_web.middleware
    async def process(self, request: aiohttp_web.Request, handler: AioHTTPHandler) -> aiohttp_web.Response:
        try:
            result = await self._rate_limiter.check_limit(
                request=request_rate_limiter.Request(
                    url=str(request.path),
                    method=request.method,
                    headers=request.headers,
                )
            )
            if not result:
                logger.warning("Request was rate limited")
                return aiohttp_web.json_response(
                    status=429,
                    data={"description": "Too Many Requests"},
                )
        except Exception as exc:
            logger.warning("Failed to check request limit", exc_info=exc)

        logger.info("No request limit was found")
        return await handler(request)


__all__ = [
    "AioHTTPMiddleware",
]
