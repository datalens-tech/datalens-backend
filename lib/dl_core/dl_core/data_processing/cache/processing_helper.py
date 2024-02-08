from __future__ import annotations

import enum
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    ClassVar,
    Optional,
    Tuple,
)

import attr

from dl_core.data_processing.streaming import AsyncChunked


if TYPE_CHECKING:
    from dl_cache_engine.engine import (
        EntityCacheEngineAsync,
        EntityCacheEntryManagerAsyncBase,
    )
    from dl_cache_engine.primitives import BIQueryCacheOptions
    from dl_core.data_processing.types import TJSONExtChunkStream
    from dl_core.services_registry import ServicesRegistry


LOGGER = logging.getLogger(__name__)


@enum.unique
class CacheSituation(enum.IntEnum):
    cache_disabled = enum.auto()
    cache_error = enum.auto()
    full_hit = enum.auto()
    generated = enum.auto()
    # TODO: discern some other cases:
    #  * now handled as cache miss -> `generated`;
    #    * cache error-marker
    #    * cache read timeout
    #    * cache lock timeout


@attr.s
class CacheProcessingHelper:
    entity_id: str = attr.ib(kw_only=True)
    _service_registry: ServicesRegistry = attr.ib(kw_only=True)
    _cache_engine: Optional[EntityCacheEngineAsync] = attr.ib(init=False, default=None)

    error_ttl_sec: ClassVar[float] = 1.5

    def __attrs_post_init__(self) -> None:
        cache_engine_factory = self._service_registry.get_cache_engine_factory()
        if cache_engine_factory is not None:
            self._cache_engine = cache_engine_factory.get_cache_engine(entity_id=self.entity_id)

    async def get_cache_entry_manager(
        self,
        *,
        cache_options: Optional[BIQueryCacheOptions],
        allow_cache_read: bool = True,
        locked_cache: bool = False,
    ) -> Optional[EntityCacheEntryManagerAsyncBase]:
        if cache_options is None:
            return None
        if not cache_options.cache_enabled:
            return None

        cache_engine = self._cache_engine
        if cache_engine is None:
            return None

        assert cache_options.key is not None
        local_key_rep = cache_options.key

        if locked_cache:
            cache_engine = cache_engine.as_lockable()

        return cache_engine.get_cache_entry_manager(
            local_key_rep=local_key_rep,
            write_ttl_sec=cache_options.ttl_sec,
            read_extend_ttl_sec=(cache_options.ttl_sec if cache_options.refresh_ttl_on_read else None),
            allow_cache_read=allow_cache_read,
        )

    def _dump_error_for_cache(self, err: BaseException) -> Any:
        return str(err)

    async def run_with_cache(
        self,
        *,
        generate_func: Callable[[], Awaitable[Optional[TJSONExtChunkStream]]],
        cache_options: BIQueryCacheOptions,
        allow_cache_read: bool = True,
        use_locked_cache: bool = False,
    ) -> Tuple[CacheSituation, Optional[TJSONExtChunkStream]]:
        cem = await self.get_cache_entry_manager(
            cache_options=cache_options,
            allow_cache_read=allow_cache_read,
            locked_cache=use_locked_cache,
        )
        if cem is None:
            result = await generate_func()
            return CacheSituation.cache_disabled, result

        # As a reference, the `cem` usage should be equivalent to this:
        #     result = None
        #     try:
        #         cache_result = await cem.initialize()
        #         if cache_result is None:
        #             result = await generate()
        #     finally:
        #         await cem.finalize(result=result, error=dump_error(sys.exc_info()))
        # The rest is just for verbosity.

        result_iter: Optional[TJSONExtChunkStream]
        sync_result_iter = None
        try:
            sync_result_iter = await cem.initialize()
        except BaseException as err:  # noqa
            LOGGER.error("Error during checking cache", exc_info=True)
            try:
                err_serializable = self._dump_error_for_cache(err)
                await cem.finalize(result=None, error=err_serializable)
            except Exception:  # Not skipping `CancelledError` here
                LOGGER.error("Error during finalizing cache (after an error during checking cache)", exc_info=True)
            LOGGER.debug("Going to db selector due to cache read error")
            result = await generate_func()
            return CacheSituation.cache_error, result

        if sync_result_iter is not None:
            result_data_inmem = list(sync_result_iter)  # type: ignore  # TODO: fix
            result_iter = AsyncChunked.from_chunked_iterable([result_data_inmem])
            try:
                await cem.finalize(result=None)
            except Exception:  # Not skipping `CancelledError` here
                LOGGER.error("Error during finalizing cache (after a cache hit)", exc_info=True)
            return CacheSituation.full_hit, result_iter

        LOGGER.info("Got selector result from cache engine: not found")

        result_as_list = None
        try:
            result_iter = await generate_func()
            if result_iter is not None:
                # Cache is not (currently) streamed, so have to collect the data.
                # And just in case, do this within the db-query try-block.
                result_as_list = await result_iter.all()
                result_iter = AsyncChunked.from_chunked_iterable([result_as_list])
        except BaseException as err:
            LOGGER.info("Error during generate_func, saving an error marker to cache")
            err_serializable = self._dump_error_for_cache(err)
            try:
                await cem.finalize(
                    result=None,
                    # short-lived error-result cache,
                    # to make it possible to make an informed decision,
                    # and to unlock the cache lock (if necessary)
                    error=err_serializable,
                    ttl_sec=self.error_ttl_sec,
                )
            except Exception:
                LOGGER.error("Error during finalizing cache (after a generate error)", exc_info=True)
            raise

        try:
            await cem.finalize(
                result=result_as_list,
            )
            LOGGER.info("Saved to cache")
        except Exception:
            LOGGER.error("Error during finalizing cache (after a generate success)", exc_info=True)

        return CacheSituation.generated, result_iter
