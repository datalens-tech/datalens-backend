import abc
import logging
from typing import TYPE_CHECKING, Type, Optional
import attr

from bi_core.us_manager.mutation_cache.usentry_mutation_cache import (
    GenericCacheEngine, MemoryCacheEngine, RedisCacheEngine
)
from bi_core.utils import FutureRef

if TYPE_CHECKING:
    from bi_core.services_registry.top_level import ServicesRegistry  # noqa

LOGGER = logging.getLogger(__name__)


class CacheInitializationError(Exception):
    pass


class MutationCacheEngineFactory(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_cache_engine(self) -> GenericCacheEngine:
        pass


@attr.s
class DefaultMutationCacheEngineFactory(MutationCacheEngineFactory):
    _services_registry_ref: FutureRef['ServicesRegistry'] = attr.ib()
    cache_type: Type[GenericCacheEngine] = attr.ib()
    _saved_inmemory_engine: Optional[MemoryCacheEngine] = None

    @classmethod
    def _get_memory_cache_engine_singleton(cls) -> MemoryCacheEngine:
        if cls._saved_inmemory_engine is None:
            cls._saved_inmemory_engine = MemoryCacheEngine()
        return cls._saved_inmemory_engine

    @property
    def service_registry(self) -> 'ServicesRegistry':
        return self._services_registry_ref.ref

    def _get_redis_cache_engine(self) -> Optional[RedisCacheEngine]:
        try:
            redis_client = self.service_registry.get_mutations_redis_client()
            if redis_client is None:
                return None
            return RedisCacheEngine(redis_client)
        except ValueError:
            return None

    def get_cache_engine(self) -> GenericCacheEngine:
        if self.cache_type == MemoryCacheEngine:
            return self._get_memory_cache_engine_singleton()
        elif self.cache_type == RedisCacheEngine:
            redis_cache_engine = self._get_redis_cache_engine()
            if redis_cache_engine:
                return redis_cache_engine
            LOGGER.info("Can not create mutation cache engine: service registry did not return a Redis client")
            raise CacheInitializationError('Cannot create mutation cache engine')
        else:
            raise CacheInitializationError('No initialization for this type of Cache Engine in factory')
