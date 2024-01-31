from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncGenerator, List, Optional, Sequence, Tuple

import redis.asyncio
import attr

from .types import Protocol
from .utils import PreExitable

if TYPE_CHECKING:
    from contextlib import AsyncExitStack  # pylint: disable=ungrouped-imports

    from redis.asyncio import Redis

    from .types import TClientACM


async def eval_script(cli: Redis, script: Any, keys: List[Any], args: List[Any]) -> Any:
    return await cli.eval(script, len(keys), *(keys + args))


def make_simple_cli_acm(url: str) -> TClientACM:
    """Single-host Redis Client ACM that supports both aioredis 1 and aioredis 2"""

    @asynccontextmanager
    async def cli_acm(**_: Any) -> AsyncGenerator[Redis, None]:
        # Redis client bound to pool of connections (auto-reconnecting).
        # Reference: https://aioredis.readthedocs.io/en/latest/migration/#connecting-to-redis
        pool = redis.asyncio.ConnectionPool.from_url(url)
        rcli = redis.asyncio.Redis(connection_pool=pool)
        try:
            yield rcli
        finally:
            await pool.disconnect()

    return cli_acm


class TSentinel(Protocol):
    def master_for(self, service_name: str) -> Redis:
        pass

    def slave_for(self, service_name: str) -> Redis:
        pass


async def make_sentinel(
    sentinels: Sequence[Tuple[str, int]], **kwargs: Any
) -> TSentinel:
    return redis.asyncio.sentinel.Sentinel(sentinels, **kwargs)


def make_sentinel_cli_acm(sentinel_cli: TSentinel, service_name: str) -> TClientACM:
    @asynccontextmanager
    async def sentinel_client_acm(
        *,
        master: bool = True,
        # aioredis does its own tracking of client reusability, so no need
        # to consider the `exclusive` flag.
        # pylint: disable=unused-argument
        exclusive: bool = True,
    ) -> AsyncGenerator[Redis, None]:
        if master:
            cli = sentinel_cli.master_for(service_name)
        else:
            cli = sentinel_cli.slave_for(service_name)
        yield cli

    return sentinel_client_acm


@attr.s(auto_attribs=True, frozen=True, slots=True)
class SubscriptionManagerBase:
    """Helper to manage a redis subscription"""

    cli: Redis
    cli_cm: PreExitable
    psub: Any
    psub_cm: PreExitable

    @classmethod
    @asynccontextmanager
    async def _psub_acm(
        cls,
        cli: Redis,  # pylint: disable=unused-argument
        channel_key: str,  # pylint: disable=unused-argument
    ) -> AsyncGenerator[Any, None]:
        if False:  # ensure this is a generator (for mypy)  # pylint: disable=using-constant-test
            yield None
        raise NotImplementedError

    @classmethod
    async def create(
        cls,
        cm_stack: AsyncExitStack,
        client_acm: TClientACM,
        channel_key: str,
    ) -> SubscriptionManagerBase:
        cli_cm = PreExitable(client_acm(master=True, exclusive=True))
        cli: Redis = await cm_stack.enter_async_context(cli_cm)
        psub_cm = PreExitable(cls._psub_acm(cli=cli, channel_key=channel_key))
        psub: Any = await cm_stack.enter_async_context(psub_cm)
        return cls(
            cli=cli,
            cli_cm=cli_cm,
            psub=psub,
            psub_cm=psub_cm,
        )

    async def get_direct(self, timeout: float) -> Optional[bytes]:
        raise NotImplementedError

    async def get(self, timeout: float) -> Optional[bytes]:
        try:
            return await asyncio.wait_for(
                self.get_direct(timeout=timeout), timeout=timeout
            )
        except asyncio.TimeoutError:
            return None

    async def exit(self) -> None:
        """
        End the subscription and release the client.
        Can be called multiple times.
        Allows freeing of resources before the passed `cm_stack` finishes.
        """
        # This is done in order and the exceptions are passed through.
        # The fallback closing is through the `cm_stack` which is done fully
        # even if some CM raises.
        await self.psub_cm.exit()
        await self.cli_cm.exit()


@attr.s(auto_attribs=True, frozen=True, slots=True)
class SubscriptionManagerLegacy(SubscriptionManagerBase):
    @classmethod
    @asynccontextmanager
    async def _psub_acm(
        cls,
        cli: Redis,
        channel_key: str,
    ) -> AsyncGenerator[Any, None]:
        try:
            channels: Tuple[Any, ...] = await cli.psubscribe(channel_key)
            if len(channels) != 1:
                raise ValueError(
                    f"Expected a single channel; "
                    f"channel_key={channel_key!r}, channels={channels!r}"
                )
            channel = channels[0]
            yield channel
        finally:
            await cli.punsubscribe(channel_key)

    async def get_direct(self, timeout: float) -> Optional[bytes]:
        try:
            # returns a `channel_key, message_data` tuple.
            item = await self.psub.get()
        except Exception:  # pylint: disable=broad-except
            # doesn't really matter which exception,
            # although this can often be `aioredis.errors.ChannelClosedError`.
            item = None

        if item is None:
            return None

        _, message = item
        return message


class SubscriptionManager(SubscriptionManagerBase):
    @classmethod
    @asynccontextmanager
    async def _psub_acm(
        cls,
        cli: Redis,
        channel_key: str,
    ) -> AsyncGenerator[Any, None]:
        async with cli.pubsub() as psub:
            await psub.subscribe(channel_key)
            try:
                yield psub
            finally:
                await psub.unsubscribe(channel_key)

    async def get_direct(self, timeout: float) -> Optional[bytes]:
        t01 = time.monotonic()
        # Apparently, `ignore_subscribe_messages=True` doesn't make the
        # `get_message` wait further; instead, it makes `get_message` return a
        # `None`.
        while True:
            message = await self.psub.get_message(
                ignore_subscribe_messages=True, timeout=timeout
            )
            if message is not None:
                break
            if time.monotonic() - t01 >= timeout:
                break
            await asyncio.sleep(0.001)
        if message is None:
            return None
        return message["data"]

    @classmethod
    async def create(
        cls,
        cm_stack: AsyncExitStack,
        client_acm: TClientACM,
        channel_key: str,
    ) -> SubscriptionManagerBase:
        """Convenience override for aioredis versions here"""
        return await super().create(
            cm_stack=cm_stack, client_acm=client_acm, channel_key=channel_key
        )
