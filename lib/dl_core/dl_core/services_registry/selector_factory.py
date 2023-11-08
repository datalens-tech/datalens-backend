from __future__ import annotations

import abc
import asyncio
import logging
from typing import (
    TYPE_CHECKING,
    List,
)

import attr

from dl_api_commons.base_models import RequestContextInfo
from dl_core.data_processing.selectors.dataset_base import DatasetDataSelectorAsyncBase
from dl_core.data_processing.selectors.db import DatasetDbDataSelectorAsync
from dl_core.us_dataset import Dataset
from dl_core.us_manager.local_cache import USEntryBuffer
from dl_core.utils import FutureRef


if TYPE_CHECKING:
    from dl_core.services_registry.top_level import ServicesRegistry


LOGGER = logging.getLogger(__name__)


class SelectorFactory(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_dataset_selector(
        self,
        dataset: Dataset,
        *,
        us_entry_buffer: USEntryBuffer,
        allow_cache_usage: bool = True,  # TODO: Remove cache from selectors
    ) -> DatasetDataSelectorAsyncBase:
        pass

    @abc.abstractmethod
    async def close_async(self) -> None:
        pass


@attr.s(frozen=True)
class BaseClosableSelectorFactory(SelectorFactory, metaclass=abc.ABCMeta):
    _services_registry_ref: FutureRef[ServicesRegistry] = attr.ib()
    _created_dataset_selectors: List[DatasetDataSelectorAsyncBase] = attr.ib(factory=list)

    @property
    def services_registry(self) -> ServicesRegistry:
        return self._services_registry_ref.ref

    @property
    def rci(self) -> RequestContextInfo:
        return self.services_registry.rci

    def get_dataset_selector(
        self,
        dataset: Dataset,
        *,
        us_entry_buffer: USEntryBuffer,
        allow_cache_usage: bool = True,
    ) -> DatasetDataSelectorAsyncBase:
        selector = self._create_dataset_selector(
            dataset=dataset,
            us_entry_buffer=us_entry_buffer,
            allow_cache_usage=allow_cache_usage,
        )
        self._created_dataset_selectors.append(selector)

        return selector

    @abc.abstractmethod
    def _create_dataset_selector(
        self,
        dataset: Dataset,
        *,
        us_entry_buffer: USEntryBuffer,
        allow_cache_usage: bool = True,
    ) -> DatasetDataSelectorAsyncBase:
        pass

    async def close_async(self) -> None:
        async def close_selector(s: "DatasetDataSelectorAsyncBase") -> None:
            # noinspection PyBroadException
            try:
                await s.close()
            except Exception:
                LOGGER.exception("Error during selector closing")

        await asyncio.gather(*[close_selector(selector) for selector in self._created_dataset_selectors])


@attr.s(frozen=True)
class DefaultSelectorFactory(BaseClosableSelectorFactory):
    _is_bleeding_edge_user: bool = attr.ib(default=False)

    def _create_dataset_selector(
        self,
        dataset: Dataset,
        *,
        us_entry_buffer: USEntryBuffer,
        allow_cache_usage: bool = True,
    ) -> DatasetDataSelectorAsyncBase:
        return DatasetDbDataSelectorAsync(
            dataset=dataset,
            service_registry=self.services_registry,
            # allow_cache_usage=allow_cache_usage,
            # is_bleeding_edge_user=self._is_bleeding_edge_user,
            us_entry_buffer=us_entry_buffer,
        )
