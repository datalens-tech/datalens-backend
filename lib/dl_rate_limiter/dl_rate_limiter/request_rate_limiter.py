import logging
import re
import typing

import attr

import dl_rate_limiter.event_rate_limiter as event_rate_limiter


logger = logging.getLogger(__name__)


class TemplateError(Exception):
    pass


@attr.s(auto_attribs=True)
class Request:
    url: str
    method: str
    headers: typing.Mapping[str, str] = attr.ib(factory=dict)


@attr.s(auto_attribs=True)
class RequestEventKeyTemplate:
    key: str
    headers: frozenset[str]

    def generate_key(self, request: Request) -> str:
        try:
            headers_values = ":".join(request.headers[header] for header in self.headers)
        except KeyError as exc:
            raise TemplateError("Header not found in request headers") from exc

        return f"{self.key}:{headers_values}"


@attr.s(auto_attribs=True)
class RequestPattern:
    url_regex: re.Pattern
    methods: frozenset[str]
    event_key_template: RequestEventKeyTemplate
    limit: int
    window_ms: int

    def matches(self, request: Request) -> bool:
        return self.url_regex.match(request.url) is not None and request.method in self.methods


class SyncRequestRateLimiterProtocol(typing.Protocol):
    def check_limit(self, request: Request) -> bool:
        ...


class AsyncRequestRateLimiterProtocol(typing.Protocol):
    async def check_limit(self, request: Request) -> bool:
        ...


@attr.s(auto_attribs=True)
class SyncRequestRateLimiter:
    _event_limiter: event_rate_limiter.SyncEventRateLimiterProtocol
    _patterns: list[RequestPattern] = attr.ib(factory=list)

    def check_limit(self, request: Request) -> bool:
        logger.info("Checking rate limit for Request(%s)", Request)
        for pattern in self._patterns:
            if not pattern.matches(request):
                continue

            logger.info("Found match for Pattern(%s)", pattern)
            try:
                event_key = pattern.event_key_template.generate_key(request)
            except TemplateError:
                logger.exception(f"Template error for request {request}")
                continue

            result = self._event_limiter.check_limit(
                event_key=event_key,
                limit=pattern.limit,
                window_ms=pattern.window_ms,
            )

            if result is False:
                return False

        return True


@attr.s(auto_attribs=True)
class AsyncRequestRateLimiter:
    _event_limiter: event_rate_limiter.AsyncEventRateLimiterProtocol
    _patterns: list[RequestPattern] = attr.ib(factory=list)

    async def check_limit(self, request: Request) -> bool:
        logger.info("Checking rate limit for Request(%s)", Request)
        for pattern in self._patterns:
            if not pattern.matches(request):
                continue

            logger.info("Found match for Pattern(%s)", pattern)
            try:
                event_key = pattern.event_key_template.generate_key(request)
            except TemplateError:
                logger.exception(f"Template error for request {request}")
                continue

            result = await self._event_limiter.check_limit(
                event_key=event_key,
                limit=pattern.limit,
                window_ms=pattern.window_ms,
            )

            if result is False:
                return False

        return True


__all__ = [
    "AsyncRequestRateLimiter",
    "SyncRequestRateLimiter",
    "RequestEventKeyTemplate",
    "RequestPattern",
    "Request",
    "AsyncRequestRateLimiterProtocol",
    "SyncRequestRateLimiterProtocol",
]
