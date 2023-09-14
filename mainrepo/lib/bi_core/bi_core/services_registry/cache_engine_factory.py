from __future__ import annotations

import abc
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Optional,
)

import attr

from bi_core.data_processing.cache.engine import EntityCacheEngineAsync
from bi_core.utils import FutureRef

if TYPE_CHECKING:
    from .top_level import ServicesRegistry  # noqa


LOGGER = logging.getLogger(__name__)


class CacheEngineFactory(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_cache_engine(self, entity_id: Optional[str]) -> Optional[EntityCacheEngineAsync]:
        pass


@attr.s
class DefaultCacheEngineFactory(CacheEngineFactory):
    _services_registry_ref: FutureRef["ServicesRegistry"] = attr.ib()
    cache_save_background: Optional[bool] = attr.ib(default=None)

    @property
    def service_registry(self) -> "ServicesRegistry":
        return self._services_registry_ref.ref

    def get_cache_engine(self, entity_id: Optional[str]) -> Optional[EntityCacheEngineAsync]:
        if not entity_id:
            LOGGER.info("Can not create entity cache engine: no entity_id")
            return None

        redis_client = self.service_registry.get_caches_redis_client()
        if redis_client is None:
            LOGGER.info("Can not create entity cache engine: service registry did not return a Redis client")
            return None

        redis_client_slave = self.service_registry.get_caches_redis_client(allow_slave=True)

        # Avoid unexpectedly overriding the default of the `EntityCacheEngineAsync` itself.
        kwargs: Dict[str, Any] = {}
        c_s_bg = self.cache_save_background
        if c_s_bg is not None:
            kwargs.update(CACHE_SAVE_BACKGROUND=c_s_bg)

        return EntityCacheEngineAsync(
            entity_id=entity_id,
            rc=redis_client,
            rc_slave=redis_client_slave,
            **kwargs,
        )
