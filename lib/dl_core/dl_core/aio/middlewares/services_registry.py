from __future__ import annotations

from aiohttp import web
from aiohttp.typedefs import (
    Handler,
    Middleware,
)

from dl_core.aio.aiohttp_wrappers_data_core import DLRequestDataCore
from dl_core.services_registry.sr_factories import SRFactory
from dl_core.us_manager.mutation_cache.usentry_mutation_cache_factory import DefaultUSEntryMutationCacheFactory


def services_registry_middleware(
    services_registry_factory: SRFactory,
    use_query_cache: bool = True,
    use_mutation_cache: bool = False,
    mutation_cache_default_ttl: float = 60,
) -> Middleware:
    """

    :param services_registry_factory:
    :param use_query_cache: If `True` - Redis client factory will be passed in service registry, Otherwise - `None`.
        Warning: if Redis service is not configured for aiohttp application and this flag is `True` -
        attempt to call `services_registry.get_caches_redis_client()` will cause an exception.
    :return: Configured middleware that creates service registry for each request.
    """

    @web.middleware
    @DLRequestDataCore.use_dl_request
    async def actual_services_registry_middleware(
        dl_request: DLRequestDataCore, handler: Handler
    ) -> web.StreamResponse:
        mutations_cache_factory, mutations_redis_client_factory = (
            (DefaultUSEntryMutationCacheFactory(default_ttl=mutation_cache_default_ttl), dl_request.get_mutations_redis)
            if use_mutation_cache
            else (None, None)
        )
        sr = services_registry_factory.make_service_registry(
            request_context_info=dl_request.rci,
            caches_redis_client_factory=dl_request.get_caches_redis if use_query_cache else None,
            mutations_cache_factory=mutations_cache_factory,
            mutations_redis_client_factory=mutations_redis_client_factory,
            data_processor_service_factory=dl_request.get_data_processor_service,
            reporting_registry=dl_request.reporting_registry,
        )
        try:
            dl_request.services_registry = sr
            return await handler(dl_request.request)
        finally:
            await sr.close_async()

    return actual_services_registry_middleware
