import abc
import logging
from typing import TYPE_CHECKING

import attr

from dl_cache_engine.cache_invalidation.engine import (
    CacheInvalidationEngine,
    StaleWhileRevalidateRedis,
)
from dl_core.utils import FutureRef


if TYPE_CHECKING:
    from dl_core.services_registry.top_level import ServicesRegistry


LOGGER = logging.getLogger(__name__)


class CacheInvalidationEngineFactory(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_cache_engine(self) -> CacheInvalidationEngine | None:
        pass


@attr.s
class DefaultCacheInvalidationEngineFactory(CacheInvalidationEngineFactory):
    _services_registry_ref: FutureRef["ServicesRegistry"] = attr.ib()

    @property
    def service_registry(self) -> "ServicesRegistry":
        return self._services_registry_ref.ref

    def get_cache_engine(self) -> CacheInvalidationEngine | None:
        redis_client = self.service_registry.get_cache_invalidations_redis_client()
        if redis_client is None:
            LOGGER.info("Cannot create invalidation cache engine: service registry did not return a Redis client")
            return None

        return CacheInvalidationEngine(
            redis=StaleWhileRevalidateRedis(redis_client=redis_client),
        )
