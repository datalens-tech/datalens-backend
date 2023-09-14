from __future__ import annotations

import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from bi_core.lifecycle.base import EntryLifecycleManager
    from bi_core.services_registry import ServicesRegistry
    from bi_core.us_entry import USEntry
    from bi_core.us_manager.us_manager import USManagerBase


class EntryLifecycleManagerFactoryBase(abc.ABC):
    @abc.abstractmethod
    def get_lifecycle_manager(
        self,
        entry: USEntry,
        us_manager: USManagerBase,
        service_registry: ServicesRegistry,
    ) -> EntryLifecycleManager:
        pass
