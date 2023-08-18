from __future__ import annotations

import abc
import asyncio
import logging
from typing import TYPE_CHECKING, List

import attr

from bi_constants.enums import SelectorType

from bi_api_commons.base_models import RequestContextInfo
from bi_core.data_processing.selectors.dataset_base import DatasetDataSelectorAsyncBase
from bi_core.data_processing.selectors.dataset_cached import CachedDatasetDataSelectorAsync
from bi_core.data_processing.selectors.dataset_cached_lazy import LazyCachedDatasetDataSelectorAsync
from bi_core.us_dataset import Dataset
from bi_core.us_manager.local_cache import USEntryBuffer
from bi_core.utils import FutureRef

if TYPE_CHECKING:
    from .top_level import ServicesRegistry  # noqa


LOGGER = logging.getLogger(__name__)


class SelectorFactory(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_dataset_selector(
            self,
            dataset: Dataset,
            selector_type: SelectorType,
            *,
            us_entry_buffer: USEntryBuffer,
            allow_cache_usage: bool = True,
    ) -> DatasetDataSelectorAsyncBase:
        pass

    @abc.abstractmethod
    async def close_async(self) -> None:
        pass


@attr.s(frozen=True)
class BaseClosableSelectorFactory(SelectorFactory, metaclass=abc.ABCMeta):
    _services_registry_ref: FutureRef['ServicesRegistry'] = attr.ib()
    _created_dataset_selectors: List[DatasetDataSelectorAsyncBase] = attr.ib(factory=list)

    @property
    def services_registry(self) -> 'ServicesRegistry':
        return self._services_registry_ref.ref

    @property
    def rci(self) -> RequestContextInfo:
        return self.services_registry.rci

    def get_dataset_selector(
            self,
            dataset: Dataset,
            selector_type: SelectorType,
            *,
            us_entry_buffer: USEntryBuffer,
            allow_cache_usage: bool = True,
    ) -> DatasetDataSelectorAsyncBase:
        selector = self._create_dataset_selector(dataset, selector_type, us_entry_buffer=us_entry_buffer)
        self._created_dataset_selectors.append(selector)

        return selector

    @abc.abstractmethod
    def _create_dataset_selector(
            self,
            dataset: Dataset,
            selector_type: SelectorType,
            *,
            us_entry_buffer: USEntryBuffer,
            allow_cache_usage: bool = True,
    ) -> DatasetDataSelectorAsyncBase:
        pass

    async def close_async(self) -> None:
        async def close_selector(s: 'DatasetDataSelectorAsyncBase') -> None:
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
            selector_type: SelectorType,
            *,
            us_entry_buffer: USEntryBuffer,
            allow_cache_usage: bool = True,
    ) -> DatasetDataSelectorAsyncBase:
        if selector_type == SelectorType.CACHED:
            return CachedDatasetDataSelectorAsync(  # type: ignore  # TODO: fix
                dataset=dataset,
                service_registry=self.services_registry,
                allow_cache_usage=allow_cache_usage,
                is_bleeding_edge_user=self._is_bleeding_edge_user,
                us_entry_buffer=us_entry_buffer,
            )
        elif selector_type == SelectorType.CACHED_LAZY:
            return LazyCachedDatasetDataSelectorAsync(  # type: ignore  # TODO: fix
                dataset=dataset,
                service_registry=self.services_registry,
                allow_cache_usage=allow_cache_usage,
                is_bleeding_edge_user=self._is_bleeding_edge_user,
                us_entry_buffer=us_entry_buffer,
            )
        else:
            raise NotImplementedError(f"Creation of selector with type {selector_type} is not supported")
