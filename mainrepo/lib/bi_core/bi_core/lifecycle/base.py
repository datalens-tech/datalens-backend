from __future__ import annotations

import abc
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Generic,
    Type,
    TypeVar,
)

import attr

from bi_core.us_entry import USEntry

if TYPE_CHECKING:
    from bi_core.services_registry.top_level import ServicesRegistry
    from bi_core.us_manager.us_manager import USManagerBase


_US_ENTRY_TV = TypeVar("_US_ENTRY_TV", bound=USEntry)


@attr.s
class EntryLifecycleManager(abc.ABC, Generic[_US_ENTRY_TV]):
    ENTRY_CLS: ClassVar[Type[_US_ENTRY_TV]]

    _entry: _US_ENTRY_TV = attr.ib(kw_only=True)
    _us_manager: USManagerBase = attr.ib(kw_only=True)
    _service_registry: ServicesRegistry = attr.ib(kw_only=True)

    @property
    def entry(self) -> _US_ENTRY_TV:
        assert isinstance(self._entry, self.ENTRY_CLS)
        return self._entry

    def pre_save_hook(self) -> None:
        pass

    def post_save_hook(self) -> None:
        pass

    def post_copy_hook(self) -> None:
        pass

    def pre_delete_hook(self) -> None:
        pass

    def post_delete_hook(self) -> None:
        pass

    async def post_init_async_hook(self) -> None:
        pass

    async def post_exec_async_hook(self) -> None:
        pass

    def collect_links(self) -> dict[str, str]:
        return {}
