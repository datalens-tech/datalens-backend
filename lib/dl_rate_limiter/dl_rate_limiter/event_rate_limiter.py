import logging
import typing

import attr
import redis
import redis.asyncio


logger = logging.getLogger(__name__)


# In case of any changes in the function code the function name should be versioned
# to avoid conflicts with the previous version of the application
ADD_EVENT_FUNCTION_NAME = "add_event"

ADD_EVENT_CODE = f"""#!lua name=ADD_REQUEST

local function {ADD_EVENT_FUNCTION_NAME}(keys, args)
    local now = redis.call('TIME')
    local now_us = now[1] .. now[2]
    local event_key = keys[1]
    
    local limit = tonumber(args[1])
    local window_ms = tonumber(args[2]) 
    
    local window_start_us = now_us - window_ms * 1000
    redis.call('ZREMRANGEBYSCORE', event_key, 0, window_start_us)
    
    local event_count = redis.call('ZCARD', event_key)
    
    if event_count >= limit then
        return 0;
    end
    
    redis.call('ZADD', event_key, now_us, now_us)
    redis.call('PEXPIRE', event_key, window_ms)
    
    return limit - event_count;
end

redis.register_function('{ADD_EVENT_FUNCTION_NAME}', {ADD_EVENT_FUNCTION_NAME})
"""


class SyncEventRateLimiterProtocol(typing.Protocol):
    def check_limit(self, event_key: str, limit: int, window_ms: int) -> bool:
        ...

    def prepare(self) -> None:
        ...


class AsyncEventRateLimiterProtocol(typing.Protocol):
    async def check_limit(self, event_key: str, limit: int, window_ms: int) -> bool:
        ...

    async def prepare(self) -> None:
        ...


@attr.s(auto_attribs=True)
class SyncRedisEventRateLimiter:
    _redis_client: redis.Redis

    def prepare(self) -> None:
        self._load_function()

    def check_limit(self, event_key: str, limit: int, window_ms: int) -> bool:
        if limit <= 0:
            return False

        if window_ms <= 0:
            return True

        try:
            result = self._redis_client.fcall(  # type: ignore # TODO: "Redis" has no attribute "fcall"  [attr-defined]
                ADD_EVENT_FUNCTION_NAME,
                1,
                event_key,
                limit,
                window_ms,
            )
        except redis.exceptions.ResponseError as exc:
            if exc.args[0].startswith("Function not found"):
                logger.info("Rate limit function not found")
                self._load_function()
                return True

            raise

        if result == 0:
            return False

        return True

    def _load_function(self, replace: bool = True) -> None:
        try:
            self._redis_client.function_load(code=ADD_EVENT_CODE, replace=replace)  # type: ignore # TODO: "Redis" has no attribute "function_load"  [attr-defined]
        except redis.exceptions.ResponseError as exc:
            logger.exception("Failed to load rate limit function")
            raise exc

        logger.info("Rate limit function loaded")


@attr.s(auto_attribs=True)
class AsyncRedisEventRateLimiter:
    _redis_client: redis.asyncio.Redis

    async def prepare(self) -> None:
        await self._load_function()

    async def check_limit(self, event_key: str, limit: int, window_ms: int) -> bool:
        if limit <= 0:
            return False

        if window_ms <= 0:
            return True

        try:
            result = await self._redis_client.fcall(
                ADD_EVENT_FUNCTION_NAME,
                1,
                event_key,  # type: ignore # TODO: fcall has incompatible type "str"; expected "list[Any]
                limit,  # type: ignore # TODO: fcall has incompatible type "str"; expected "list[Any]
                window_ms,  # type: ignore # TODO: fcall has incompatible type "str"; expected "list[Any]
            )
        except redis.exceptions.ResponseError as exc:
            if exc.args[0].startswith("Function not found"):
                logger.info("Rate limit function not found")
                await self._load_function()
                return True

            raise

        if result == 0:
            return False

        return True

    async def _load_function(self, replace: bool = True) -> None:
        try:
            await self._redis_client.function_load(code=ADD_EVENT_CODE, replace=replace)
        except redis.exceptions.ResponseError as exc:
            logger.exception("Failed to load rate limit function")
            raise exc

        logger.info("Rate limit function loaded")


__all__ = [
    "AsyncRedisEventRateLimiter",
    "AsyncEventRateLimiterProtocol",
    "SyncEventRateLimiterProtocol",
    "SyncRedisEventRateLimiter",
]
