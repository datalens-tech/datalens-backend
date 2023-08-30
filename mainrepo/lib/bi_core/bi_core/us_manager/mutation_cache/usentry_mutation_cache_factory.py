import abc
import logging
from typing import TYPE_CHECKING, Optional

import attr

from bi_core.us_manager.mutation_cache.usentry_mutation_cache import USEntryMutationCache, GenericCacheEngine
from ..us_manager import USManagerBase

if TYPE_CHECKING:
    from bi_core.services_registry.top_level import ServicesRegistry  # noqa

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
