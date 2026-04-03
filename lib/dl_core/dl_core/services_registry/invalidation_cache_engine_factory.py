from __future__ import annotations

import abc
import logging
from typing import (
    TYPE_CHECKING,
    Optional,
)

import attr

from dl_cache_engine.invalidation import InvalidationCacheEngine
from dl_core.utils import FutureRef


if TYPE_CHECKING:
    from dl_core.services_registry.top_level import ServicesRegistry


LOGGER = logging.getLogger(__name__)


class InvalidationCacheEngineFactory(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_cache_engine(self) -> Optional[InvalidationCacheEngine]:
        pass


@attr.s
class DefaultInvalidationCacheEngineFactory(InvalidationCacheEngineFactory):
    _services_registry_ref: FutureRef[ServicesRegistry] = attr.ib()

    @property
    def service_registry(self) -> ServicesRegistry:
        return self._services_registry_ref.ref

    def get_cache_engine(self) -> Optional[InvalidationCacheEngine]:
        redis_client = self.service_registry.get_cache_invalidations_redis_client()
        if redis_client is None:
            LOGGER.info("Can not create invalidation cache engine: service registry did not return a Redis client")
            return None

        return InvalidationCacheEngine(
            redis_client=redis_client,
        )
