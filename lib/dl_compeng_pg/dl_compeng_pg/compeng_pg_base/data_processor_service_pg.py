# TODO: Move to data_processing
from __future__ import annotations

import abc
import logging
from typing import (
    ClassVar,
    TypeVar,
)

import attr

from dl_compeng_pg.compeng_pg_base.pool_base import (
    DEFAULT_OPERATION_TIMEOUT,
    DEFAULT_POOL_MAX_SIZE,
    DEFAULT_POOL_MIN_SIZE,
    BasePgPoolWrapper,
)
from dl_core.aio.web_app_services.data_processing.data_processor import (
    DataProcessorConfig,
    DataProcessorService,
)
from dl_core.utils import secrepr_db_url

LOGGER = logging.getLogger(__name__)


@attr.s(frozen=True)
class CompEngPgConfig(DataProcessorConfig):
    url: str = attr.ib(kw_only=True)


_COMPENG_PR_SRV_TV = TypeVar("_COMPENG_PR_SRV_TV", bound="CompEngPgService")


@attr.s
class CompEngPgService[POOL_TV: BasePgPoolWrapper](DataProcessorService, metaclass=abc.ABCMeta):
    APP_KEY: ClassVar[str] = "compeng_service"

    _pg_url: str = attr.ib(repr=secrepr_db_url)  # DSN
    _pool_min_size: int = attr.ib(default=DEFAULT_POOL_MIN_SIZE)
    _pool_max_size: int = attr.ib(default=DEFAULT_POOL_MAX_SIZE)
    _operation_timeout: float = attr.ib(default=DEFAULT_OPERATION_TIMEOUT)
    _pool: POOL_TV | None = attr.ib(init=False, default=None)

    @property
    def pool(self) -> POOL_TV:
        if self._pool is None:
            raise ValueError("Pool was not created")
        return self._pool

    async def initialize(self) -> None:
        await self._init_pool()

    @abc.abstractmethod
    def _get_pool_wrapper_cls(self) -> type[BasePgPoolWrapper]:
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
        assert self._pool is not None
        LOGGER.info("Tear down compeng pg pool %r...", self)
        await self._pool.disconnect()
        self._pool = None
        LOGGER.info("Tear down compeng pg pool %r: done.", self)

    @classmethod
    def from_config(cls: type[_COMPENG_PR_SRV_TV], config: DataProcessorConfig) -> _COMPENG_PR_SRV_TV:
        assert isinstance(config, CompEngPgConfig)
        return cls(
            pg_url=config.url,
        )
