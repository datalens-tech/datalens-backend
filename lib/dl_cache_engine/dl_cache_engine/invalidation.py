"""
Invalidation Cache Engine.

Stores and retrieves invalidation payloads in a dedicated Redis DB.
The payload is a short string (e.g. first row of a lightweight "invalidation query")
that is used as part of the main cache key. When the payload changes,
the main cache key changes → cache miss → fresh data is fetched.

Key format (RCL data key)::

    bic_cache_inval/data:cache_inval_{sha256(dataset_id + ":" + dataset_rev_id + ":" + conn_id + ":" + conn_rev_id)}

Value format (JSON)::

    {
        "status": "success" | "error",
        "executed_at": 1642242600,  // unix timestamp
        // On success:
        "payload": {
            "data": "<result of invalidation query>"
        },
        // On error:
        "payload": {
            "error_code": "ERR.DS....",
            "error_message": "Query timeout exceeded: 5s",
            "error_details": { ... }
        }
    }

Uses ``RedisCacheLock`` to prevent multiple workers from executing the invalidation
query simultaneously. The first request acquires the lock and executes the query;
other requests wait for the result via Pub/Sub.

Throttling is controlled by TTL: data is stored with TTL = throttling_interval_sec.
When the TTL expires, the data is automatically removed from Redis, and the next
request triggers a new invalidation query via RCL.

Flow overview (inside ``CacheExecAdapter._execute_and_fetch``)::

    1. Check if invalidation is enabled for this dataset/connection
    2. Build InvalidationCacheKey from dataset_id, ds_rev_id, conn_id, conn_rev_id
    3. Call get_or_generate() with TTL = throttling_interval_sec:
       a. RCL checks its data key — if data exists (TTL not expired), returns it
       b. If no data — first worker acquires lock, executes invalidation query, saves entry
       c. Other workers wait via Pub/Sub and receive the result
    4. Add payload as DataKeyPart("cache_invalidation_payload", payload) to main cache key
    5. Proceed with standard CacheProcessingHelper.run_with_cache()
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import time
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Optional,
)

import attr
from redis_cache_lock.main import RedisCacheLock
from redis_cache_lock.utils import HistoryHolder

from dl_app_tools.profiling_base import generic_profiler_async


if TYPE_CHECKING:
    from redis.asyncio import Redis
    from redis_cache_lock.enums import ReqResultInternal
    from redis_cache_lock.redis_utils import SubscriptionManagerBase


LOGGER = logging.getLogger(__name__)

KEY_PREFIX = "cache_inval_"
RCL_RESOURCE_TAG = "bic_cache_inval"

# RCL parameters for invalidation cache
RCL_LOCK_TTL_SEC = 30.0
RCL_NETWORK_CALL_TIMEOUT_SEC = 30.0

STATUS_SUCCESS = "success"
STATUS_ERROR = "error"


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class InvalidationCacheKey:
    """
    Components of the invalidation cache key.

    The key does NOT depend on the user's query — one invalidation cache entry
    serves ALL queries to a given dataset through a given connection.
    """

    dataset_id: str
    dataset_revision_id: str
    connection_id: str
    connection_revision_id: str

    def to_redis_key(self) -> str:
        raw = f"{self.dataset_id}:{self.dataset_revision_id}:{self.connection_id}:{self.connection_revision_id}"
        key_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()
        return f"{KEY_PREFIX}{key_hash}"


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class InvalidationSuccessPayload:
    """Payload for a successful invalidation query."""

    data: str

    def to_dict(self) -> dict[str, Any]:
        return {"data": self.data}

    @classmethod
    def from_dict(cls, data_dict: dict[str, Any]) -> InvalidationSuccessPayload:
        return cls(data=data_dict["data"])


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class InvalidationErrorPayload:
    """Payload for a failed invalidation query."""

    error_code: str
    error_message: Optional[str] = None
    error_details: dict[str, Any] = attr.Factory(dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_code": self.error_code,
            "error_message": self.error_message,
            "error_details": self.error_details,
        }

    @classmethod
    def from_dict(cls, data_dict: dict[str, Any]) -> InvalidationErrorPayload:
        return cls(
            error_code=data_dict["error_code"],
            error_message=data_dict.get("error_message"),
            error_details=data_dict.get("error_details", {}),
        )


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class InvalidationCacheEntry:
    """
    Represents a single entry in the invalidation cache.

    Attributes:
        status: "success" if the invalidation query succeeded, "error" otherwise.
        payload: On success — ``InvalidationSuccessPayload`` with the query result.
                 On error — ``InvalidationErrorPayload`` with error details.
        executed_at: Unix timestamp of when the invalidation query was last executed.
    """

    status: str  # STATUS_SUCCESS or STATUS_ERROR
    payload: InvalidationSuccessPayload | InvalidationErrorPayload
    executed_at: float  # unix timestamp

    def to_json_bytes(self) -> bytes:
        return json.dumps(
            {
                "status": self.status,
                "payload": self.payload.to_dict(),
                "executed_at": self.executed_at,
            },
            ensure_ascii=True,
        ).encode("utf-8")

    @classmethod
    def from_json_bytes(cls, data: bytes) -> InvalidationCacheEntry:
        parsed = json.loads(data.decode("utf-8"))
        status = parsed["status"]
        payload_dict = parsed["payload"]

        if status == STATUS_SUCCESS:
            payload: InvalidationSuccessPayload | InvalidationErrorPayload = InvalidationSuccessPayload.from_dict(
                payload_dict
            )
        else:
            payload = InvalidationErrorPayload.from_dict(payload_dict)

        return cls(
            status=status,
            payload=payload,
            executed_at=parsed["executed_at"],
        )

    @classmethod
    def make_success(cls, data: str) -> InvalidationCacheEntry:
        """Create a success entry with the given data payload."""
        return cls(
            status=STATUS_SUCCESS,
            payload=InvalidationSuccessPayload(data=data),
            executed_at=time.time(),
        )

    @classmethod
    def make_error(
        cls,
        error_code: str,
        error_message: Optional[str] = None,
        error_details: Optional[dict[str, Any]] = None,
    ) -> InvalidationCacheEntry:
        """Create an error entry with error details."""
        return cls(
            status=STATUS_ERROR,
            payload=InvalidationErrorPayload(
                error_code=error_code,
                error_message=error_message,
                error_details=error_details or {},
            ),
            executed_at=time.time(),
        )

    @property
    def is_success(self) -> bool:
        return self.status == STATUS_SUCCESS

    @property
    def data(self) -> Optional[str]:
        """Return the data payload if success, None otherwise."""
        if isinstance(self.payload, InvalidationSuccessPayload):
            return self.payload.data
        return None


class _RedisCacheLockInvalidation(RedisCacheLock):
    """Profiling additions to the RCL for invalidation cache operations."""

    @generic_profiler_async("inval-cache-rcl-get-data-slave")  # type: ignore  # TODO: fix
    async def get_data_slave(self) -> Optional[bytes]:
        return await super().get_data_slave()

    @generic_profiler_async("inval-cache-rcl-get-data-main")  # type: ignore  # TODO: fix
    async def _get_data(
        self,
    ) -> tuple["ReqResultInternal", Optional[bytes], Optional["SubscriptionManagerBase"]]:
        return await super()._get_data()

    @generic_profiler_async("inval-cache-rcl-wait-for-result")  # type: ignore  # TODO: fix
    async def _wait_for_result(
        self,
        sub: "SubscriptionManagerBase",
    ) -> tuple["ReqResultInternal", Optional[bytes]]:
        return await super()._wait_for_result(sub)


# Type alias for the generate function expected by get_or_generate().
# Must return (serialized_bytes, raw_value) — the RCL contract.
TInvalidationGenerateFunc = Callable[[], Awaitable[tuple[bytes, InvalidationCacheEntry]]]


@attr.s(auto_attribs=True)
class InvalidationCacheEngine:
    """
    Engine for reading/writing invalidation cache entries in a dedicated Redis DB.

    Uses ``RedisCacheLock`` to prevent concurrent execution of the same invalidation
    query by multiple workers:

    - First request acquires the lock and executes the invalidation query
    - Other requests wait for the result via Pub/Sub
    - After the result is available, all waiters use it

    Throttling is controlled by TTL: data is stored with TTL = throttling_interval_sec.
    When the TTL expires, the RCL data key is automatically removed by Redis,
    and the next ``get_or_generate()`` call triggers a new invalidation query.

    This class is a pure storage + coordination layer. It does NOT know about SQL,
    formulas, or connectors. The caller provides a ``generate_func`` that performs
    the actual invalidation query.
    """

    _redis_client: Redis = attr.ib()

    @contextlib.asynccontextmanager
    async def _with_redis(self, *, master: bool = True, exclusive: bool = True) -> AsyncGenerator[Redis, None]:
        """
        Provide a Redis client to RedisCacheLock.

        Follows the same pattern as ``EntityCacheEntryLockedManagerAsync._with_redis``.
        """
        cli = self._redis_client
        try:
            yield cli
        finally:
            await cli.aclose(close_connection_pool=False)  # type: ignore[attr-defined]  # Not relevant mypy stubs

    def _make_rcl(self, key: InvalidationCacheKey, ttl_sec: float) -> tuple[RedisCacheLock, HistoryHolder]:
        """
        Create a RedisCacheLock instance for the given invalidation cache key.

        The ``ttl_sec`` parameter controls how long the data is stored in Redis.
        It should be set to ``throttling_interval_sec`` so that the data expires
        automatically when the throttling interval elapses.

        RCL key structure::

            bic_cache_inval/data:cache_inval_{hash}
            bic_cache_inval/lock:cache_inval_{hash}
            bic_cache_inval/notif:cache_inval_{hash}
        """
        redis_key = key.to_redis_key()
        history_holder = HistoryHolder()
        rcl = _RedisCacheLockInvalidation(
            key=redis_key,
            client_acm=self._with_redis,
            resource_tag=RCL_RESOURCE_TAG,
            lock_ttl_sec=RCL_LOCK_TTL_SEC,
            data_ttl_sec=ttl_sec,
            network_call_timeout_sec=RCL_NETWORK_CALL_TIMEOUT_SEC,
            enable_background_tasks=False,
            enable_slave_get=False,
            debug_log=history_holder,
        )
        return rcl, history_holder

    @generic_profiler_async("inval-cache-get-or-generate")  # type: ignore  # TODO: fix
    async def get_or_generate(
        self,
        key: InvalidationCacheKey,
        ttl_sec: float,
        generate_func: TInvalidationGenerateFunc,
    ) -> Optional[InvalidationCacheEntry]:
        """
        Get an invalidation cache entry using RedisCacheLock for coordination.

        If the entry exists in Redis (RCL data key) and TTL has not expired,
        returns it immediately. If the TTL has expired (data auto-deleted by Redis),
        acquires a lock, calls ``generate_func`` to produce the payload,
        saves it via RCL with TTL = ``ttl_sec``, and returns the entry.

        Other concurrent callers will wait for the result via Pub/Sub.

        ``generate_func`` must return ``(serialized_bytes, entry)`` where
        ``serialized_bytes`` is ``entry.to_json_bytes()``.

        The ``ttl_sec`` should be set to ``throttling_interval_sec`` so that
        the data expires automatically when the throttling interval elapses,
        triggering a new invalidation query on the next request.

        Returns None on errors.
        """
        rcl, history_holder = self._make_rcl(key=key, ttl_sec=ttl_sec)

        try:
            result_bytes, _ = await rcl.generate_with_lock(generate_func)
        except Exception:
            LOGGER.warning(
                "Error in invalidation cache get_or_generate key=%s",
                key.to_redis_key(),
                exc_info=True,
            )
            self._log_rcl_history(key, history_holder)
            return None

        if result_bytes is None:
            return None

        try:
            return InvalidationCacheEntry.from_json_bytes(result_bytes)
        except (json.JSONDecodeError, KeyError, ValueError):
            LOGGER.warning(
                "Corrupted invalidation cache entry from RCL key=%s",
                key.to_redis_key(),
                exc_info=True,
            )
            return None

    @staticmethod
    def _log_rcl_history(key: InvalidationCacheKey, history_holder: HistoryHolder) -> None:
        """Log RCL debug history for troubleshooting."""
        events = history_holder.history
        if events:
            min_ts = min((ts for ts, _, _ in events), default=0)
            events_readable = ";  ".join(f"{round(ts - min_ts, 3)}: {msg}" for ts, msg, _ in events)
            LOGGER.debug(
                "RedisCacheLock (invalidation) logs for key=%s: %d events: %s",
                key.to_redis_key(),
                len(events),
                events_readable,
            )
            history_holder.history = []
