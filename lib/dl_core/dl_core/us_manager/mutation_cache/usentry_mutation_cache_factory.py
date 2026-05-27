import abc
import logging
from typing import Optional

import attr

from dl_core.us_manager.mutation_cache.usentry_mutation_cache import (
    GenericCacheEngine,
    USEntryMutationCache,
)
from dl_core.us_manager.us_manager import USManagerBase

LOGGER = logging.getLogger(__name__)


@attr.s
class USEntryMutationCacheFactory(metaclass=abc.ABCMeta):
    default_ttl: float = attr.ib(default=60)

    @abc.abstractmethod
    def get_mutation_cache(
        self,
        usm: USManagerBase,
        engine: GenericCacheEngine,
        ttl: Optional[float] = None,
    ) -> USEntryMutationCache:
        pass


@attr.s
class DefaultUSEntryMutationCacheFactory(USEntryMutationCacheFactory):
    def get_mutation_cache(
        self,
        usm: USManagerBase,
        engine: GenericCacheEngine,
        ttl: Optional[float] = None,
    ) -> USEntryMutationCache:
        if ttl is None:
            ttl = self.default_ttl
        return USEntryMutationCache(
            usm=usm,
            cache_engine=engine,
            default_ttl=ttl,
        )
