# TODO: Move to data_processing
from __future__ import annotations

import abc
import logging
from typing import Generic, ClassVar, Optional, Type, TypeVar

import attr

from bi_core.aio.web_app_services.data_processing.data_processor import DataProcessorService, DataProcessorConfig
from bi_compeng_pg.compeng_pg_base.pool_base import (
    BasePgPoolWrapper,
    DEFAULT_POOL_MIN_SIZE, DEFAULT_POOL_MAX_SIZE, DEFAULT_OPERATION_TIMEOUT,
)

LOGGER = logging.getLogger(__name__)


@attr.s(frozen=True)
class CompEngPgConfig(DataProcessorConfig):
    url: str = attr.ib(kw_only=True)


_POOL_TV = TypeVar("_POOL_TV", bound=BasePgPoolWrapper)
_COMPENG_PR_SRV_TV = TypeVar("_COMPENG_PR_SRV_TV", bound='CompEngPgService')


@attr.s
class CompEngPgService(DataProcessorService, Generic[_POOL_TV], metaclass=abc.ABCMeta):
    APP_KEY: ClassVar[str] = 'compeng_service'

    _pg_url: str = attr.ib()  # DSN
    _pool_min_size: int = attr.ib(default=DEFAULT_POOL_MIN_SIZE)
    _pool_max_size: int = attr.ib(default=DEFAULT_POOL_MAX_SIZE)
    _operation_timeout: float = attr.ib(default=DEFAULT_OPERATION_TIMEOUT)
    _pool: Optional[_POOL_TV] = attr.ib(init=False, default=None)

    @property
    def pool(self) -> _POOL_TV:
        if self._pool is None:
            raise ValueError("Pool was not created")
        return self._pool

    async def initialize(self) -> None:
        await self._init_pool()

    @abc.abstractmethod
    def _get_pool_wrapper_cls(self) -> Type[BasePgPoolWrapper]:
        raise NotImplementedError

    async def _init_pool(self) -> None:
        self._pool = await self._get_pool_wrapper_cls().connect(  # type: ignore  # TODO: fix
            self._pg_url,
            pool_min_size=self._pool_min_size,
            pool_max_size=self._pool_max_size,
            operation_timeout=self._operation_timeout,
        )

    async def finalize(self) -> None:
        if self._pool is not None:
            await self._deinit_pool()

    async def _deinit_pool(self) -> None:
        """ ... """
        LOGGER.info("Tear down compeng pg pool %r...", self)
        await self._pool.disconnect()  # type: ignore  # TODO: fix
        self._pool = None
        LOGGER.info("Tear down compeng pg pool %r: done.", self)

    @classmethod
    def from_config(cls: Type[_COMPENG_PR_SRV_TV], config: DataProcessorConfig) -> _COMPENG_PR_SRV_TV:
        assert isinstance(config, CompEngPgConfig)
        return cls(
            pg_url=config.url,
        )
