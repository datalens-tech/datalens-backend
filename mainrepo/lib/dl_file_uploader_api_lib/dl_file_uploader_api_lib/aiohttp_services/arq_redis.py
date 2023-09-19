from __future__ import annotations

import logging
from typing import (
    ClassVar,
    Optional,
)

from aiohttp import web
import arq
from arq.connections import RedisSettings as ArqRedisSettings
import attr

from dl_task_processor.arq_wrapper import create_redis_pool


LOGGER = logging.getLogger(__name__)


@attr.s
class ArqRedisService:
    APP_KEY: ClassVar[str] = "ARQ_REDIS_SERVICE"
    _arq_settings: ArqRedisSettings = attr.ib()
    _arq_pool: Optional[arq.ArqRedis] = attr.ib(init=False, default=None)

    async def init_hook(self, target_app: web.Application) -> None:
        target_app[self.APP_KEY] = self
        await self.initialize()

    async def tear_down_hook(self, target_app: web.Application) -> None:
        await self.tear_down()

    def get_arq_pool(self) -> arq.ArqRedis:
        assert self._arq_pool is not None
        return self._arq_pool

    @classmethod
    def get_app_instance(cls, app: web.Application) -> ArqRedisService:
        service: Optional[ArqRedisService] = app.get(cls.APP_KEY, None)
        if service is None:
            raise ValueError("Redis was not initiated for application")

        return service

    async def initialize(self) -> None:
        LOGGER.info("Initializing Redis ARQ pool")
        self._arq_pool = await create_redis_pool(self._arq_settings)

    async def tear_down(self) -> None:
        LOGGER.info("Close Redis ARQ pool")
        assert self._arq_pool is not None
        await self._arq_pool.close()
