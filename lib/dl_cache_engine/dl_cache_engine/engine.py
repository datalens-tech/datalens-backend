"""

Terminology note: to avoid confusion, using `entry` as in `cache entry`, and
`entity` as in `united-storage entity` (dataset/connection).

Logging fields:

  * Save, Read:
    * cache_key
    * connection_id
    * connection_revision
    * cache_serialized_bytesize  # serialized, without compression
    * cache_dumped_bytesize  # 'fully dumped' as in serialized + maybe-compressed
    * cache_was_compressed
    * cache_compress_alg
    * cache_row_count
    * cache_serializer_timing  # for both serialize and deserialize
    * cache_compressor_timing  # for both compress and decompress
    * cache_ttl_sec  # required on save, optional update on read

  * Save: INFO "Cache saved at {key}"

  * Read: INFO "Cache read from {key}"
    * cache_refresh_on_read

  * Read miss: "Cache miss: …: …"
    * cache_miss_reason

  * Pre-del (sync, async): INFO "Invalidating cache for entity %s"


(note that there's also GenericProfiler logging)
TODO?: gather all the profilings into a log field?


Logging fields in earlier versions:

  * Pre-save: INFO "Saving key … …"
  * Pre-save: INFO "Redis key is %s. Requesting redis"
    * cache_key
    * connection_id
    * connection_revision
    * ttl_sec
    * compress_alg
  * Pre-save: DEBUG ("Preparing cache entry %s"
  * Pre-save: Data dump (serialization + compression): INFO "Cache data was dumped"
    * rows_count
    * encoded_bytes_count
    * was_compressed
    * final_bytes_count
  * Pre-save (sync, async): INFO "Saving to redis..."

  * Pre-load: INFO "Looking for key … …"
  * Pre-load: INFO "Redis key is %s. Requesting redis..."
    * cache_key
    * connection_id
    * connection_revision
    * new_ttl_sec
    * refresh_on_read

  * Cache miss: "BI common version was updated, ignoring cache entry"
  * Cache miss: "Query hash collision, ignoring cache entry"
  * Cache miss: "Connection was updated, ignoring cache entry"

  * Pre-del (sync, async): INFO "Invalidating cache for entity %s"

"""

from __future__ import annotations

import asyncio
import collections
import contextlib
import enum
import gzip
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Deque,
    Dict,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

import attr
import lz4.frame
from redis_cache_lock.main import RedisCacheLock
from redis_cache_lock.utils import HistoryHolder

from dl_app_tools.profiling_base import (
    GenericProfiler,
    generic_profiler_async,
)
from dl_cache_engine.exc import CachedEntryPackageVersionMismatch
from dl_cache_engine.primitives import LocalKeyRepresentation
from dl_constants.types import TJSONExt
from dl_model_tools.serialization import (
    CacheMetadataSerialization,
    common_dumps,
    common_loads,
)


if TYPE_CHECKING:
    from redis.asyncio import Redis


LOGGER = logging.getLogger(__name__)


@enum.unique
class CompressAlg(enum.Enum):
    GZIP = "gzip"
    LZ4 = "lz4"


@attr.s(frozen=True, auto_attribs=True, slots=True)
class _CacheUpdateRequest:
    full_key: str
    ttl_sec: float
    entry_data: bytes
    details: Dict[str, Any]  # debug/stats, to be dumped into logging.


@attr.s(auto_attribs=True, slots=True)
class ResultCacheEntry:
    """
    ...

    Reminder: `metadata` is saved with the cache, `details` are for the logging.
    """

    key_parts_str: str
    metadata: Optional[Dict[str, Any]] = None
    details: Optional[Dict[str, Any]] = None

    _result_data: Optional[TJSONExt] = None
    _result_data_redis_repr: Optional[bytes] = None
    _is_compressed: Optional[bool] = None
    _compress_alg: Optional[CompressAlg] = None

    def __attrs_post_init__(self) -> None:
        if self._result_data is not None:
            assert self._result_data_redis_repr is None
            assert self._is_compressed is None
            self._is_compressed = False
        elif self._result_data_redis_repr is not None:
            assert self._result_data is None
            assert self._is_compressed is not None

        metadata = self.metadata
        self.metadata = metadata.copy() if metadata else {}
        details = self.details
        self.details = details.copy() if details else {}

    @staticmethod
    def bi_c_version_str() -> str:
        return "12.99.0"

    @property
    def is_error(self) -> bool:
        assert self.metadata is not None
        return self.metadata.get("error") is not None
        # TODO: require the key presence:
        # return self.metadata['error'] is not None

    def make_result_data(self) -> TJSONExt:
        """Deserialize the result data (if not saved) and update the details"""
        if self._result_data is not None:
            return self._result_data

        if not self._result_data_redis_repr:
            raise ValueError("Cache entry contain no data")

        prof_decompress = None
        if self._is_compressed:
            with GenericProfiler("qcache-decompress") as prof_decompress:
                if self._compress_alg == CompressAlg.LZ4:
                    encoded_data = lz4.frame.decompress(self._result_data_redis_repr)
                elif self._compress_alg == CompressAlg.GZIP:
                    encoded_data = gzip.decompress(self._result_data_redis_repr)
                else:
                    encoded_data = gzip.decompress(self._result_data_redis_repr)
        else:
            encoded_data = self._result_data_redis_repr

        with GenericProfiler("qcache-deserialize") as prof_deserialize:
            self._result_data = common_loads(encoded_data)

        details = self.details
        assert details is not None
        result_data = self._result_data
        assert isinstance(result_data, list)
        details.update(
            cache_dumped_bytesize=len(self._result_data_redis_repr),
            cache_was_compressed=self._is_compressed,
            cache_compress_alg=(self._compress_alg.name if self._compress_alg is not None else None),
            cache_compressor_timing=(prof_decompress.exec_time_sec if prof_decompress is not None else None),
            cache_serialized_bytesize=len(encoded_data),
            cache_serializer_timing=prof_deserialize.exec_time_sec,
            cache_row_count=len(result_data),
        )
        return self._result_data

    @classmethod
    def from_redis_data(
        cls,
        redis_data: bytes,
        details: Optional[Dict[str, Any]] = None,
    ) -> ResultCacheEntry:
        ser_obj = CacheMetadataSerialization(redis_data)
        metadata = ser_obj.metadata
        assert isinstance(metadata, dict)
        initial_bi_c_version_str = metadata["bi_common_version"]

        if initial_bi_c_version_str != cls.bi_c_version_str():
            raise CachedEntryPackageVersionMismatch(
                "Package version mismatch. In cache: {}. Actual: {}".format(
                    initial_bi_c_version_str, cls.bi_c_version_str()
                )
            )

        is_compressed = metadata["is_compressed"]
        assert isinstance(is_compressed, bool)
        compress_alg_name = metadata.get("compress_alg")
        compress_alg: Optional[CompressAlg] = None

        if is_compressed:
            if not compress_alg_name:
                compress_alg = CompressAlg.GZIP
            else:
                compress_alg = CompressAlg(compress_alg_name)

        data = ser_obj.data_bytes
        key_parts_str = metadata["key_parts_str"]
        assert isinstance(key_parts_str, str)
        return cls(
            key_parts_str=key_parts_str,
            result_data_redis_repr=data,
            is_compressed=is_compressed,
            compress_alg=compress_alg,
            metadata=metadata,
            details=details,
        )

    def to_redis_data_and_log_details(
        self, compress: bool = False, compress_alg: Optional[CompressAlg] = None
    ) -> Tuple[bytes, Dict[str, Any]]:
        result_data = self._result_data
        assert result_data is not None
        assert isinstance(result_data, list)

        # Note: "qcache-serialize" used to be on this entire method rather than only on dumps.
        with GenericProfiler("qcache-serialize") as prof_serialize:
            serialized_result_data = common_dumps(result_data)

        result_data_to_store = serialized_result_data

        prof_compress = None
        if compress:
            if compress_alg is None:
                compress_alg = CompressAlg.GZIP

            with GenericProfiler("qcache-compress") as prof_compress:
                if compress_alg == CompressAlg.LZ4:
                    result_data_to_store = lz4.frame.compress(serialized_result_data)
                elif compress_alg == CompressAlg.GZIP:
                    result_data_to_store = gzip.compress(serialized_result_data)

        details: Dict[str, Any] = dict(
            cache_row_count=len(result_data),
            cache_serialized_bytesize=len(serialized_result_data),
            cache_serializer_timing=prof_serialize.exec_time_sec,
            cache_was_compressed=compress,
            cache_compressor_timing=(prof_compress.exec_time_sec if prof_compress is not None else None),
            cache_dumped_bytesize=len(result_data_to_store),
        )

        metadata = dict(  # TODO?: attrs-object? or protobuf?
            self.metadata or {},
            bi_common_version=self.bi_c_version_str(),
            key_parts_str=self.key_parts_str,
            is_compressed=bool(compress),
            compress_alg=compress_alg.value if compress_alg else None,
        )
        data_bytes = CacheMetadataSerialization.serialize(metadata, result_data_to_store)
        return data_bytes, details

    def to_redis_data(
        self,
        compress: bool = False,
        compress_alg: Optional[CompressAlg] = None,
    ) -> bytes:
        """
        Convenience wrapper, primarily for tests.
        """
        result, _ = self.to_redis_data_and_log_details(
            compress=compress,
            compress_alg=compress_alg,
        )
        return result


_ECEMA_TV = TypeVar("_ECEMA_TV", bound="EntityCacheEntryManagerAsyncBase")


@attr.s(auto_attribs=True, slots=True)
class EntityCacheEntryManagerAsyncBase:
    """
    Single-cache-entry manager.

    Primarily intended to support cache-entry locking,
    i.e. to provide a protocol that makes it possible.
    """

    local_key_rep: LocalKeyRepresentation
    cache_engine: EntityCacheEngineAsync
    write_ttl_sec: float
    read_extend_ttl_sec: Optional[float] = None
    allow_cache_read: bool = True

    _starting: bool = False
    _started: bool = False

    def clone(self: _ECEMA_TV, **updates: Any) -> _ECEMA_TV:
        return attr.evolve(self, **updates)

    async def initialize(self) -> Optional[TJSONExt]:
        if self._starting or self._started:
            raise Exception("Already initialized and not finalized")
        self._starting = True
        result = await self._initialize()
        self._started = True
        self._starting = False
        return result

    async def _initialize(self) -> Optional[TJSONExt]:
        raise NotImplementedError

    async def finalize(
        self,
        result: Optional[TJSONExt],
        error: Optional[Any] = None,
        ttl_sec: Optional[float] = None,
    ) -> None:
        if not self._starting and not self._started:
            raise Exception("Not initialized")
        try:
            return await self._finalize(result=result, error=error, ttl_sec=ttl_sec)
        finally:
            self._starting = False
            self._started = False

    async def _finalize(
        self,
        result: Optional[TJSONExt],
        error: Optional[Any] = None,
        ttl_sec: Optional[float] = None,
    ) -> None:
        raise NotImplementedError


@attr.s(auto_attribs=True, slots=True)
class EntityCacheEntryManagerAsync(EntityCacheEntryManagerAsyncBase):
    async def _initialize(self) -> Optional[TJSONExt]:
        if not self.allow_cache_read:
            return None
        return await self.cache_engine._get_from_cache(
            local_key_rep=self.local_key_rep,
            new_ttl_sec=self.read_extend_ttl_sec,
        )

    async def _finalize(
        self,
        result: Optional[TJSONExt],
        error: Optional[Any] = None,
        ttl_sec: Optional[float] = None,
    ) -> None:
        if result is None:
            if error is None:
                return None  # nothing to save at all
            result = []  # TODO: allow an actually null result (metadata-only)
        return await self.cache_engine._update_cache(
            local_key_rep=self.local_key_rep,
            result=result,
            metadata=dict(error=error),
            ttl_sec=ttl_sec or self.write_ttl_sec,
        )


class RedisCacheLockWrapped(RedisCacheLock):
    """Local profiling additions to the RCL"""

    @generic_profiler_async("qcache-locked-get-data-slave")  # type: ignore  # TODO: fix
    async def get_data_slave(self) -> Optional[bytes]:
        return await super().get_data_slave()

    @generic_profiler_async("qcache-locked-get-data-main")  # type: ignore  # TODO: fix
    async def _get_data(self) -> Any:
        return await super()._get_data()

    @generic_profiler_async("qcache-locked-get-data-wait")  # type: ignore  # TODO: fix
    async def _wait_for_result(self, sub: Any) -> Any:
        return await super()._wait_for_result(sub)


@attr.s(auto_attribs=True, slots=True)
class EntityCacheEntryLockedManagerAsync(EntityCacheEntryManagerAsyncBase):
    _rcl: Optional[RedisCacheLock] = None
    _history_holder: Optional[HistoryHolder] = None

    @contextlib.asynccontextmanager
    async def _with_redis(self, *, master: bool = True, exclusive: bool = True) -> AsyncGenerator[Redis, None]:
        cache_engine = self.cache_engine
        # This does not allow retries at this layer; would need the source
        # sentinel object for that.
        cli = cache_engine.rc if master else (cache_engine.rc_slave or cache_engine.rc)
        assert cli is not None
        try:
            yield cli
        finally:
            await cli.aclose(close_connection_pool=False)  # type: ignore # TODO: Not relevant mypy stubs

    def _make_rcl(self) -> Tuple[RedisCacheLock, HistoryHolder]:
        cache_engine = self.cache_engine
        local_key_rep = self.local_key_rep
        local_key_rep.validate()
        key = local_key_rep.key_parts_hash
        key_root = cache_engine._get_key_root()
        # Keys structure: `resource_tag + ('/data:'|'/notif:'|'/lock:') + key`
        # e.g. 'bic_dp_ce_l__iyirei5oqew0c__query_cache//data:657f18518eaa2f41307895e18c3ba0d12d97b8a23c6de3966f52c6ba39a07ee4'
        # NOTE: not using `cache_engine._get_key_query_cache_entry` at all
        history_holder = HistoryHolder()
        rcl = RedisCacheLockWrapped(
            key=key,
            client_acm=self._with_redis,
            resource_tag=key_root,
            lock_ttl_sec=15.0,
            data_ttl_sec=self.write_ttl_sec,
            # Not exactly the same semantics, but generally the same order of magnitude.
            # If a more precise timeout is desired, `rcl.initialize` can be
            # timeouted safely (as long as `finalize` is called).
            network_call_timeout_sec=self.cache_engine.CACHE_GET_TIMEOUT_SEC,
            enable_background_tasks=self.cache_engine.CACHE_SAVE_BACKGROUND,
            enable_slave_get=self.cache_engine.CACHE_CHECK_SLAVE,
            debug_log=history_holder,
        )
        return rcl, history_holder

    @property
    def rcl(self) -> RedisCacheLock:
        rcl = self._rcl
        if rcl is None:
            rcl, history_holder = self._make_rcl()
            self._rcl = rcl
            self._history_holder = history_holder
        return rcl

    @generic_profiler_async("qcache-locked-initialize")  # type: ignore  # TODO: fix
    async def _initialize(self) -> Optional[TJSONExt]:
        """
        WARNING: partially reimplements `EntityCacheEngineAsync._get_from_cache`.
        """
        local_key_rep = self.local_key_rep
        cache_engine = self.cache_engine
        rcl = self.rcl

        full_key, details = cache_engine._make_full_key_and_log_details(
            local_key_rep=local_key_rep,
            new_ttl_sec=self.read_extend_ttl_sec,
        )

        result = await rcl.initialize()

        situation = rcl._situation
        LOGGER.debug("Cache-locked situation=%r, have result: %r", situation, result is not None)

        cache_entry = self.cache_engine._make_result_cache_entry(
            local_key_rep=self.local_key_rep,
            cache_entry_redis_data=result,
            details=details,
        )
        if cache_entry is None:
            return None

        result_data = cache_entry.make_result_data()
        cache_engine._log_after_read(full_key=full_key, cache_entry=cache_entry)
        return result_data

    @generic_profiler_async("qcache-locked-finalize")  # type: ignore  # TODO: fix
    async def _finalize(
        self,
        result: Optional[TJSONExt],
        error: Optional[Any] = None,
        ttl_sec: Optional[float] = None,
    ) -> None:
        rcl = self._rcl
        assert rcl is not None, "should have been created by now"

        ttl_sec = ttl_sec or self.write_ttl_sec
        if result is None:
            result = []  # TODO: allow an actually null result (metadata-only)

        update_request = self.cache_engine._make_cache_update_request(
            local_key_rep=self.local_key_rep,
            result=result,
            compress=False,
            compress_alg=None,  # `compress=False` to prevent CPU-bound operation in event loop
            metadata=dict(error=error),
            ttl_sec=ttl_sec,
        )
        try:
            await rcl.finalize(update_request.entry_data, ttl_sec=ttl_sec)
        except BaseException as err:
            self.cache_engine._log_finalize_failed(update_request=update_request, err=err)
            self.save_history(error=error, save_error=err)
            raise

        self.cache_engine._log_after_finalize(update_request=update_request)
        self.save_history(error=error, save_error=None)

    def save_history(self, error: Any = None, save_error: Any = None) -> None:
        history_holder = self._history_holder
        assert history_holder
        events = history_holder.history
        history_holder.history = []
        min_ts = min((ts for ts, _, _ in events), default=0)
        events_readable = ";  ".join(f"{round(ts - min_ts, 3)}: {msg}" for ts, msg, _ in events)
        LOGGER.debug(
            "RedisCacheLock logs: %d events, err=%r, save_err=%r: %s",
            len(events),
            error,
            save_error,
            events_readable,
            extra=dict(redis_cache_lock_logs=events),
        )


_ECE_TV = TypeVar("_ECE_TV", bound="EntityCacheEngineBase")


@attr.s(auto_attribs=True)
class EntityCacheEngineBase:
    entity_id: str

    DEFAULT_TTL_SEC: float = 60.0
    DELETE_SCAN_CHUNK_SIZE: int = 100
    DEFAULT_COMPRESS_ALG: CompressAlg = CompressAlg.GZIP
    MIN_BYTES_TO_COMPRESS: int = 120

    # Old keys, effectively, were 'dataset:{entity_id}:query_cache:'
    key_prefix_tpl: str = "bic_dp_ce__{entity_id}__query_cache/"  # dl_core data_processing cache_engine
    data_keys_suffix: str = ""

    def clone(self: _ECE_TV, **updates: Any) -> _ECE_TV:
        return attr.evolve(self, **updates)

    # Keys section
    def _get_key_root(self) -> str:
        return self.key_prefix_tpl.format(entity_id=self.entity_id)

    def _get_key_query_cache_entry(self, local_key_rep: LocalKeyRepresentation) -> str:
        local_key_rep.validate()
        # noinspection PyProtectedMember
        return "{entity_root_key}/{local_key_rep_str}".format(
            entity_root_key=self._get_key_root(),
            local_key_rep_str=local_key_rep.key_parts_hash,
        )

    def _get_all_keys_pattern(self) -> str:
        """Returns pattern for all cached keys for the entity"""
        return self._get_key_root() + self.data_keys_suffix + "*"

    def _make_cache_update_request(
        self,
        local_key_rep: LocalKeyRepresentation,
        result: TJSONExt,
        compress: bool = False,
        compress_alg: Optional[CompressAlg] = None,
        ttl_sec: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> _CacheUpdateRequest:
        if ttl_sec is None:
            ttl_sec = self.DEFAULT_TTL_SEC

        if compress_alg is None:
            compress_alg = self.DEFAULT_COMPRESS_ALG

        full_key = self._get_key_query_cache_entry(local_key_rep=local_key_rep)

        # noinspection PyProtectedMember
        entry_data, details = ResultCacheEntry(
            key_parts_str=local_key_rep.key_parts_str,
            result_data=result,
            metadata=metadata,
        ).to_redis_data_and_log_details(
            compress,
            compress_alg=compress_alg,
        )

        # Enrich the debug/stats details for after-success log:
        details = dict(
            details,
            cache_key=full_key,
            cache_ttl_sec=ttl_sec,
            cache_compress_alg=compress_alg.name,
        )

        return _CacheUpdateRequest(
            full_key=full_key,
            ttl_sec=ttl_sec,
            entry_data=entry_data,
            details=details,
        )

    def _make_full_key_and_log_details(
        self,
        local_key_rep: LocalKeyRepresentation,
        new_ttl_sec: Optional[float] = None,
    ) -> Tuple[str, dict]:
        full_key = self._get_key_query_cache_entry(local_key_rep)

        details = dict(
            cache_key=full_key,
            cache_refresh_on_read=new_ttl_sec is not None,
            cache_ttl_sec=new_ttl_sec,
        )

        return full_key, details

    def _make_result_cache_entry(
        self,
        local_key_rep: LocalKeyRepresentation,
        cache_entry_redis_data: Optional[bytes],
        details: Dict[str, Any],
        allow_error_entry: bool = False,
    ) -> Optional[ResultCacheEntry]:
        if not cache_entry_redis_data:
            self._log_cache_miss(reason="no_data", details=details)
            return None

        try:
            cache_entry = ResultCacheEntry.from_redis_data(
                cache_entry_redis_data,
                details=details,
            )
        except CachedEntryPackageVersionMismatch as err:
            self._log_cache_miss(
                reason="bi_common_was_updated",
                details=details,
                message=str(err),
            )
            return None

        if cache_entry.key_parts_str != local_key_rep.key_parts_str:
            self._log_cache_miss(
                reason="query_hash_collision",
                details=details,
                message="Query hash collision: {!r} != {!r}".format(
                    cache_entry.key_parts_str, local_key_rep.key_parts_str
                ),
            )
            return None

        if not allow_error_entry:
            if cache_entry.is_error:
                metadata = cache_entry.metadata
                assert metadata is not None
                self._log_cache_miss(
                    reason="error_mark",
                    details=dict(details, error=metadata["error"]),
                )
                return None

        return cache_entry

    def _log_after_save(self, update_request: _CacheUpdateRequest) -> None:
        LOGGER.info("Cache saved at %r", update_request.full_key, extra=update_request.details)

    def _log_save_failed(self, update_request: _CacheUpdateRequest, err: BaseException) -> None:
        LOGGER.info("Cache save failed at %r: %r", update_request.full_key, err, extra=update_request.details)

    def _log_after_finalize(self, update_request: _CacheUpdateRequest) -> None:
        LOGGER.info("Cache finalized at %r", update_request.full_key, extra=update_request.details)

    def _log_finalize_failed(self, update_request: _CacheUpdateRequest, err: BaseException) -> None:
        LOGGER.info("Cache finalize failed at %r: %r", update_request.full_key, err, extra=update_request.details)

    def _log_after_read(self, full_key: str, cache_entry: ResultCacheEntry) -> None:
        LOGGER.info("Cache read from %r", full_key, extra=cache_entry.details)

    def _log_cache_miss(self, reason: str, details: Dict[str, Any], message: Optional[str] = None) -> None:
        full_key = details["cache_key"]
        details = dict(details, cache_miss_reason=reason)
        LOGGER.info("Cache miss at %r: %r: %r", full_key, reason, message or "-", extra=details)

    def _log_cache_timeout(self, timeout: float, details: Dict[str, Any], message: Optional[str] = None) -> None:
        full_key = details["cache_key"]
        details = dict(
            details,
            cache_read_timeout=timeout,
        )
        LOGGER.info("Cache timeout at %r: %r", full_key, message or "-", extra=details)


@attr.s(auto_attribs=True)
class EntityCacheEngineAsync(EntityCacheEngineBase):
    """
    ...

    WARNING: this is, mostly, an async+await copy of the `EntityCacheEngine`,
    requires a different class of the redis client,
    does not compress on save.
    """

    rc: Redis = attr.ib(kw_only=True)
    rc_slave: Optional[Redis] = None

    CACHE_GET_TIMEOUT_SEC: float = 5.0
    CACHE_SAVE_TIMEOUT_SEC: float = 30.0  # should not be a problem since it happens in background
    CACHE_SAVE_BACKGROUND: bool = True
    CACHE_CHECK_SLAVE: bool = False  # does not go well when the slave is in another country.

    entity_cache_entry_manager_cls: Type[EntityCacheEntryManagerAsyncBase] = EntityCacheEntryManagerAsync

    def as_lockable(self) -> EntityCacheEngineAsync:
        """
        Use the entry manager that synchronizes the data generation over a lock,
        and a different key prefix just in case.
        """
        return self.clone(
            entity_cache_entry_manager_cls=EntityCacheEntryLockedManagerAsync,
            # dl_core data_processing cache_engine locked
            key_prefix_tpl="bic_dp_ce_l__{entity_id}__query_cache/",
            data_keys_suffix="/data:*",
        )

    # Redis helpers
    @generic_profiler_async("qcache-invalidate")  # type: ignore  # TODO: fix
    async def _delete_keys_by_pattern(self, pattern: str) -> None:
        """SCAN is used to prevent redis lockout with KEYS command."""
        LOGGER.info("Invalidating cache for entity %s", self.entity_id)
        all_keys: Deque[bytes] = collections.deque()
        cur = 0
        started = False
        while cur or not started:
            started = True
            cur, keys = await self.rc.scan(cur, match=pattern, count=self.DELETE_SCAN_CHUNK_SIZE)
            for single_key in keys:
                all_keys.append(single_key)

        pipeline = self.rc.pipeline()
        for key in all_keys:
            pipeline.delete(key)
        await pipeline.execute()

    @generic_profiler_async("qcache-update")  # type: ignore  # TODO: fix
    async def _update_cache(
        self,
        local_key_rep: LocalKeyRepresentation,
        result: TJSONExt,
        metadata: Optional[Dict[str, Any]] = None,
        ttl_sec: Optional[float] = None,
    ) -> None:
        update_request = self._make_cache_update_request(
            local_key_rep=local_key_rep,
            result=result,
            compress=False,
            compress_alg=None,  # `compress=False` to prevent CPU-bound operation in event loop
            ttl_sec=ttl_sec,
        )

        if self.CACHE_SAVE_BACKGROUND:
            task_tmp = asyncio.create_task(self._redis_set(update_request))
            await asyncio.shield(task_tmp)
        else:
            try:
                await asyncio.wait_for(
                    self._redis_set(update_request),
                    timeout=self.CACHE_SAVE_TIMEOUT_SEC,
                )
            except asyncio.TimeoutError as err:
                self._log_save_failed(update_request=update_request, err=err)

    @generic_profiler_async("qcache-write-redis-exec")  # type: ignore  # TODO: fix
    async def _redis_set(self, update_request: _CacheUpdateRequest) -> None:
        try:
            rcli = self.rc
            pipe = rcli.pipeline()
            pipe.set(update_request.full_key, update_request.entry_data)
            pipe.pexpire(update_request.full_key, int(update_request.ttl_sec * 1000))
            await pipe.execute()
        except Exception as err:
            self._log_save_failed(update_request=update_request, err=err)
        else:
            self._log_after_save(update_request=update_request)

    @generic_profiler_async("qcache-get")  # type: ignore  # TODO: fix
    async def _get_from_cache(
        self,
        local_key_rep: LocalKeyRepresentation,
        new_ttl_sec: Optional[float] = None,
    ) -> Optional[TJSONExt]:
        full_key, details = self._make_full_key_and_log_details(local_key_rep=local_key_rep, new_ttl_sec=new_ttl_sec)

        read_timeout = self.CACHE_GET_TIMEOUT_SEC
        try:
            cache_entry_redis_data = await asyncio.wait_for(
                self._redis_get(full_key, new_ttl_sec=new_ttl_sec),
                timeout=read_timeout,
            )
        except asyncio.TimeoutError:
            self._log_cache_timeout(timeout=read_timeout, details=details)
            return None

        cache_entry = self._make_result_cache_entry(
            local_key_rep=local_key_rep,
            cache_entry_redis_data=cache_entry_redis_data,
            details=details,
        )

        if cache_entry is None:  # cache miss, supposed to be logged by now.
            return None

        # deserializes data and also mutates `cache_entry.details`.
        result = cache_entry.make_result_data()
        self._log_after_read(full_key=full_key, cache_entry=cache_entry)
        return result

    @generic_profiler_async("qcache-read-redis-exec")  # type: ignore  # TODO: fix
    async def _redis_get(
        self,
        full_key: str,
        new_ttl_sec: Optional[float] = None,
    ) -> Optional[bytes]:
        rcli = self.rc
        if new_ttl_sec is not None:
            pipe = rcli.pipeline()
            pipe.pexpire(full_key, int(new_ttl_sec * 1000))
            pipe.get(full_key)
            _, result = await pipe.execute()
            return result

        rc_slave = self.rc_slave
        if self.CACHE_CHECK_SLAVE and rc_slave is not None:
            result = await rc_slave.get(full_key)
            if result is not None:
                return result

        result = await rcli.get(full_key)
        return result

    async def invalidate_all(self) -> None:
        await self._delete_keys_by_pattern(self._get_all_keys_pattern())

    def get_cache_entry_manager(
        self,
        local_key_rep: LocalKeyRepresentation,
        **kwargs: Any,
    ) -> Optional[EntityCacheEntryManagerAsyncBase]:
        return self.entity_cache_entry_manager_cls(
            local_key_rep=local_key_rep,
            cache_engine=self,
            **kwargs,
        )
