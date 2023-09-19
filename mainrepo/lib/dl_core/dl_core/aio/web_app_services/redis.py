from __future__ import annotations

import abc
import logging
from typing import (
    ClassVar,
    Optional,
    Sequence,
)

from aiohttp import web
import attr
import redis.asyncio
from redis.asyncio.sentinel import Sentinel as RedisSentinel
from redis.asyncio.sentinel import SentinelConnectionPool

from dl_constants.enums import RedisInstanceKind
from dl_core.utils import secrepr


LOGGER = logging.getLogger(__name__)


@attr.s(kw_only=True)
class RedisConnParams:
    host: str = attr.ib()
    port: int = attr.ib()
    db: int = attr.ib()
    password: str = attr.ib(repr=secrepr)
    ssl: Optional[bool] = attr.ib()


async def extract_redis_conn_params(redis_cli: Optional[redis.asyncio.Redis]) -> Optional[RedisConnParams]:
    if redis_cli is None:
        return None
    redis_conn_params = redis_cli.connection_pool.connection_kwargs
    host = redis_conn_params.get("host")
    port = redis_conn_params.get("port")

    if "connection_pool" in redis_conn_params:
        conn_pool: SentinelConnectionPool = redis_conn_params["connection_pool"]
        master_address = await conn_pool.get_master_address()
        host, port = master_address

    assert host is not None
    assert port is not None

    return RedisConnParams(
        host=host,
        port=port,
        db=redis_conn_params["db"],
        password=redis_conn_params["password"],
        ssl=redis_conn_params.get("ssl"),
    )


@attr.s
class RedisBaseService(metaclass=abc.ABCMeta):
    APP_KEY: ClassVar[str] = "REDIS_SERVICE"
    _instance_kind: RedisInstanceKind = attr.ib()
    _ssl: Optional[bool] = attr.ib(default=None)

    async def init_hook(self, target_app: web.Application) -> None:
        LOGGER.info(f"Initializing Redis {self._instance_kind.name}")
        target_app[self.get_full_app_key(self._instance_kind)] = self
        await self.initialize()

    async def tear_down_hook(self, target_app: web.Application) -> None:
        await self.tear_down()

    @abc.abstractmethod
    def get_redis(self, allow_slave: bool = False) -> redis.asyncio.Redis:
        pass

    @classmethod
    def get_full_app_key(cls, instance_kind: RedisInstanceKind) -> str:
        return f"{cls.APP_KEY}_{instance_kind.name}"

    @classmethod
    def get_app_instance(cls, app: web.Application, instance_kind: RedisInstanceKind) -> "RedisBaseService":
        service = app.get(cls.get_full_app_key(instance_kind), None)
        if service is None:
            raise ValueError("Redis was not initiated for application")

        return service

    @abc.abstractmethod
    async def initialize(self) -> None:
        pass

    @abc.abstractmethod
    async def tear_down(self) -> None:
        pass


@attr.s(kw_only=True)
class RedisSentinelService(RedisBaseService):
    _sentinel_hosts: Sequence[str] = attr.ib()
    _sentinel_port: int = attr.ib()
    _namespace: str = attr.ib()
    _db: int = attr.ib()
    _password: str = attr.ib(repr=False)
    _cached_master: Optional[redis.asyncio.Redis] = attr.ib(init=False, default=None)
    _cached_slave: Optional[redis.asyncio.Redis] = attr.ib(init=False, default=None)

    _sentinel_client: RedisSentinel = attr.ib(init=False, repr=False, hash=False, cmp=False)

    async def initialize(self) -> None:
        LOGGER.info("Internal initializing Redis Sentinels")
        self._sentinel_client = RedisSentinel(
            [(h, self._sentinel_port) for h in self._sentinel_hosts],
            db=self._db,
            password=self._password,
            ssl=self._ssl,
        )
        await self._sentinel_client.discover_master(self._namespace)

    async def tear_down(self) -> None:
        if self._cached_slave:
            await self._cached_slave.close()
        if self._cached_master:
            await self._cached_master.close()

    def get_redis(self, allow_slave: bool = False) -> redis.asyncio.Redis:
        sentinel_cli = self._sentinel_client
        if allow_slave:
            cached_redis = self._cached_slave
            if cached_redis is None:
                cached_redis = sentinel_cli.slave_for(self._namespace)
                self._cached_slave = cached_redis
        else:
            cached_redis = self._cached_master
            if cached_redis is None:
                cached_redis = sentinel_cli.master_for(self._namespace)
                self._cached_master = cached_redis
        return cached_redis


@attr.s(kw_only=True)
class SingleHostSimpleRedisService(RedisBaseService):
    _url: str = attr.ib()
    _password: Optional[str] = attr.ib(default=None, repr=False)

    _redis_pool: redis.asyncio.ConnectionPool = attr.ib(init=False, repr=False, hash=False, cmp=False)

    async def initialize(self) -> None:
        LOGGER.info("Internal initializing Redis Single Host")
        self._redis_pool = redis.asyncio.ConnectionPool.from_url(self._url, password=self._password)

    async def tear_down(self) -> None:
        LOGGER.info("Tear down redis")
        LOGGER.info("Waiting for redis pool to become closed")
        await self._redis_pool.disconnect()
        LOGGER.info("Redis pool become closed")

    def get_redis(self, allow_slave: bool = False) -> redis.asyncio.Redis:
        return redis.asyncio.Redis(connection_pool=self._redis_pool)
