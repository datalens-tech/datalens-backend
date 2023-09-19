from __future__ import annotations

import abc
import asyncio
import logging
from typing import (
    TYPE_CHECKING,
    Any,
)

import attr

from dl_constants.enums import ProcessorType
from dl_core.data_processing.processing.processor import OperationProcessorAsyncBase
from dl_core.us_dataset import Dataset
from dl_core.us_manager.local_cache import USEntryBuffer
from dl_core.utils import FutureRef


if TYPE_CHECKING:
    from dl_core.services_registry.top_level import ServicesRegistry  # noqa


LOGGER = logging.getLogger(__name__)


@attr.s(frozen=True)
class DataProcessorFactory(metaclass=abc.ABCMeta):
    _services_registry_ref: FutureRef["ServicesRegistry"] = attr.ib(kw_only=True)

    @property
    def services_registry(self) -> "ServicesRegistry":
        return self._services_registry_ref.ref

    @abc.abstractmethod
    async def get_data_processor(
        self,
        dataset: Dataset,
        processor_type: ProcessorType,
        *,
        us_entry_buffer: USEntryBuffer,
        allow_cache_usage: bool = True,
        **kwargs: Any,
    ) -> OperationProcessorAsyncBase:
        pass

    @abc.abstractmethod
    async def close_async(self) -> None:
        pass


@attr.s(frozen=True)
class BaseClosableDataProcessorFactory(DataProcessorFactory):
    _created_data_processors: list[OperationProcessorAsyncBase] = attr.ib(factory=list, init=False)

    async def get_data_processor(
        self,
        dataset: Dataset,
        processor_type: ProcessorType,
        *,
        us_entry_buffer: USEntryBuffer,
        allow_cache_usage: bool = True,
        **kwargs: Any,
    ) -> OperationProcessorAsyncBase:
        processor = self._create_data_processor(
            dataset,
            processor_type,
            us_entry_buffer=us_entry_buffer,
            allow_cache_usage=allow_cache_usage,
            **kwargs,
        )
        self._created_data_processors.append(processor)

        await processor.start()
        return processor

    @abc.abstractmethod
    def _create_data_processor(  # type: ignore  # TODO: fix
        self,
        dataset: Dataset,
        processor_type: ProcessorType,
        *,
        us_entry_buffer: USEntryBuffer,
        allow_cache_usage: bool = True,
        **kwargs,
    ) -> OperationProcessorAsyncBase:
        pass

    async def close_async(self) -> None:
        async def close_processor(s: "OperationProcessorAsyncBase") -> None:
            # noinspection PyBroadException
            try:
                await s.end()
            except Exception:
                LOGGER.exception("Error during processor closing")

        await asyncio.gather(*[close_processor(processor) for processor in self._created_data_processors])
