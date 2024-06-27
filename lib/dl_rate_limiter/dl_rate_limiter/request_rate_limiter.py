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
class RequestEventKeyTemplateHeader:
    key: str
    regex: typing.Optional[re.Pattern] = None  # Should contain result group

    def generate_value(self, headers: typing.Mapping[str, str]) -> str:
        try:
            value = headers[self.key]
        except KeyError as exc:
            raise TemplateError(f"Header {self.key} not found in request headers") from exc

        if self.regex is None:
            return value

        match = self.regex.match(value)
        if match is None:
            raise TemplateError(f"Header {self.key} value {value} does not match regex {self.regex}")

        result = match.group("result")

        if result is None:
            raise TemplateError(f"Header {self.key} value {value} does not contain result group")

        return result


@attr.s(auto_attribs=True)
class RequestEventKeyTemplate:
    key: str
    headers: tuple[RequestEventKeyTemplateHeader, ...]
    header_value_separator: str = "%%"

    def generate_key(self, request: Request) -> str:
        headers_values = self.header_value_separator.join(
            header.generate_value(request.headers) for header in self.headers
        )

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
            except TemplateError as exc:
                logger.warning("Template error for request", exc_info=exc)
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
            except TemplateError as exc:
                logger.warning("Template error for request", exc_info=exc)
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
    "RequestEventKeyTemplateHeader",
    "RequestEventKeyTemplate",
    "RequestPattern",
    "Request",
    "AsyncRequestRateLimiterProtocol",
    "SyncRequestRateLimiterProtocol",
]
