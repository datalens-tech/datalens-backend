from __future__ import annotations

import abc
import logging
from typing import (
    ClassVar,
    Type,
    TypeVar,
    cast,
)

from aiohttp import web
import attr

from bi_core.data_processing.processing.processor import OperationProcessorAsyncBase

LOGGER = logging.getLogger(__name__)


@attr.s(frozen=True)
class DataProcessorConfig:
    pass


_DATA_PROC_SRV_TV = TypeVar("_DATA_PROC_SRV_TV", bound="DataProcessorService")


@attr.s
class DataProcessorService(metaclass=abc.ABCMeta):
    APP_KEY: ClassVar[str]

    @classmethod
    def get_full_app_key(cls) -> str:
        return cls.APP_KEY

    async def init_hook(self, target_app: web.Application) -> None:
        assert self.APP_KEY not in target_app, f"{type(self).__name__} already initiated for app"
        LOGGER.info(f"Initializing {type(self).__name__} data processor service")
        target_app[self.APP_KEY] = self
        await self.initialize()

    async def tear_down_hook(self, target_app: web.Application) -> None:
        LOGGER.info("Tear down {type(self).__name__} data processor service...")
        await self.tear_down(target_app)
        LOGGER.info("Tear down {type(self).__name__} data processor service: done.")

    async def tear_down(self, target_app: web.Application) -> None:
        await self.finalize()

    @abc.abstractmethod
    async def initialize(self) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    async def finalize(self) -> None:
        raise NotImplementedError

    @classmethod
    def get_app_instance(cls: Type[_DATA_PROC_SRV_TV], app: web.Application) -> _DATA_PROC_SRV_TV:
        service = cast(_DATA_PROC_SRV_TV, app.get(cls.get_full_app_key(), None))
        if service is None:
            raise ValueError(f"{cls.__name__} was not initiated for application")

        return service

    @abc.abstractmethod
    def get_data_processor(self) -> OperationProcessorAsyncBase:
        raise NotImplementedError

    @classmethod
    def from_config(cls: Type[_DATA_PROC_SRV_TV], config: DataProcessorConfig) -> _DATA_PROC_SRV_TV:
        return cls()
