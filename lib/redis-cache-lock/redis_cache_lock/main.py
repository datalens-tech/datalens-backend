from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    AsyncGenerator,
    Callable,
    ClassVar,
    Dict,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

import attr

from .enums import ReqResultInternal, ReqScriptResult
from .exc import NetworkCallTimeoutError
from .redis_utils import SubscriptionManager
from .scripts import ALIVE_PREFIX, DATA_PREFIX, FAIL_PREFIX
from .scripts_support import (
    FailScript,
    ForceSaveScript,
    RenewScript,
    ReqScript,
    SaveScript,
)
from .utils import await_on_exit, new_self_id, task_cm

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from .redis_utils import SubscriptionManagerBase
    from .types import TCacheResult, TClientACM, TGenerateFunc, TGenerateResult


_WNC_RET_T = TypeVar("_WNC_RET_T")


@attr.s(auto_attribs=True)
class RedisCacheLock:
    client_acm: TClientACM = attr.ib(repr=False)

    key: str
    resource_tag: str  # namespace for the keys
    data_ttl_sec: float

    lock_ttl_sec: float = 3.5
    lock_renew_interval: Optional[float] = None
    network_call_timeout_sec: float = 2.5

    debug_log: Optional[Callable[[str, Dict[str, Any]], None]] = attr.ib(default=None, repr=False)
    bg_task_callback: Optional[Callable[[asyncio.Task], None]] = attr.ib(default=None, repr=False)
    enable_background_tasks: bool = False
    enable_slave_get: bool = True

    req_script_cls: ClassVar[Type[ReqScript]] = ReqScript
    req_script_situation: ClassVar[Type[ReqScriptResult]] = ReqScriptResult
    req_situation: ClassVar[Type[ReqResultInternal]] = ReqResultInternal
    renew_script_cls: ClassVar[Type[RenewScript]] = RenewScript
    save_script_cls: ClassVar[Type[SaveScript]] = SaveScript
    fail_script_cls: ClassVar[Type[FailScript]] = FailScript
    force_save_script_cls: ClassVar[Type[ForceSaveScript]] = ForceSaveScript
    chan_data_prefix: ClassVar[bytes] = DATA_PREFIX
    chan_alive_prefix: ClassVar[bytes] = ALIVE_PREFIX
    chan_fail_prefix: ClassVar[bytes] = FAIL_PREFIX
    subscription_manager_cls: Type[SubscriptionManagerBase] = SubscriptionManager

    data_tag: str = "/data:"
    signal_tag: str = "/notif:"
    lock_tag: str = "/lock:"

    # TODO: split this class into a config + CM pair

    _situation: Optional[ReqResultInternal] = None
    _cm_stack: Optional[AsyncExitStack] = None
    _client: Optional[Redis] = None
    _self_id: Optional[str] = None

    @staticmethod
    def new_self_id() -> str:
        return new_self_id()

    @property
    def situation(self) -> Optional[ReqResultInternal]:
        return self._situation

    @situation.setter
    def situation(self, value: ReqResultInternal) -> None:
        if value == self._situation:
            return
        self._log("Setting situation to %r", value, situation=value)
        self._situation = value

    def clone(self, **kwargs: Any) -> RedisCacheLock:
        result = attr.evolve(self, **kwargs)
        result._cleanup()  # pylint: disable=protected-access
        return result

    def _cleanup(self) -> None:
        self._client = None
        self._cm_stack = None
        self._self_id = None
        self._situation = None

    def _log(self, msg: str, *args: Any, **details: Any) -> None:
        if self.debug_log is not None:
            self.debug_log(  # pylint: disable=not-callable
                msg % args,
                dict(
                    details,
                    key=self.key,
                    self_id=self._self_id,
                ),
            )

    @property
    def data_key(self) -> str:
        return self.resource_tag + self.data_tag + self.key

    @property
    def signal_key(self) -> str:
        return self.resource_tag + self.signal_tag + self.key

    @property
    def lock_key(self) -> str:
        return self.resource_tag + self.lock_tag + self.key

    @asynccontextmanager
    async def _client_acm_managed(
        self, master: bool = True, exclusive: bool = True
    ) -> AsyncGenerator[Redis, None]:
        async with AsyncExitStack() as temp_cm_stack:  # easier way to timeout a CM
            client: Redis = await self._wait_network_call(
                temp_cm_stack.enter_async_context(
                    self.client_acm(master=master, exclusive=exclusive)
                )
            )
            yield client

    def process_in_background(self, coro: Awaitable, name: Optional[str] = None) -> Any:
        """An overridable method for finalization background-task creation"""
        if name is None:
            name = repr(coro)

        async def background_wrapper() -> Any:
            try:
                return await coro
            except Exception as err:  # pylint: disable=broad-except
                self._log("Exception in background task %r: %r", name, err)
                return None

        task = asyncio.create_task(background_wrapper(), name=name)
        callback = self.bg_task_callback
        if callback is not None:
            callback(task)  # pylint: disable=not-callable
        return task

    async def _wait_network_call(self, coro: Awaitable[_WNC_RET_T]) -> _WNC_RET_T:
        try:
            return await asyncio.wait_for(coro, timeout=self.network_call_timeout_sec)
        except TimeoutError as err:
            raise NetworkCallTimeoutError() from err

    async def _finalize_maybe_in_background(
        self,
        coro: Awaitable,
        name: Optional[str] = None,
    ) -> Tuple[bool, Any]:
        """
        Depending on the settings, either await the `coro` directly (but with cancellation shield),
        or run it in a background asyncio task.
        """
        if not self.enable_background_tasks:
            # The `wait_for(shield(coro))` results in waiting until timeout but
            # leaving the coro in background when the timeout is reached.
            result = await self._wait_network_call(asyncio.shield(coro))
            return False, result

        return True, await asyncio.shield(self.process_in_background(coro, name=name))

    async def _postpone_to_finalization(self, coro: Awaitable) -> Any:
        cm_stack = self._cm_stack
        assert cm_stack is not None, "must be initialized for this method"
        return await cm_stack.enter_async_context(await_on_exit(coro))

    async def get_data_slave(self) -> Optional[bytes]:
        data_key = self.data_key
        cli: Redis
        async with self._client_acm_managed(master=False) as cli:
            return await self._wait_network_call(cli.get(data_key))

    async def _get_data(
        self,
    ) -> Tuple[ReqResultInternal, Optional[bytes], Optional[SubscriptionManagerBase]]:
        self_id = self._self_id
        assert self_id is not None, "must be initialized for this method"
        cm_stack = self._cm_stack
        assert cm_stack is not None, "must be initialized for this method"
        cli = self._client
        assert cli is not None, "must be initialized for this method"

        subscription: Optional[SubscriptionManagerBase] = None

        lock_key = self.lock_key
        data_key = self.data_key
        lock_ttl_sec = self.lock_ttl_sec

        req_script = self.req_script_cls(cli=cli)

        self._log("Calling req_script", lock_ttl_sec=lock_ttl_sec)
        self.situation = self.req_situation.requesting
        situation, result = await self._wait_network_call(
            req_script(
                lock_key=lock_key,
                data_key=data_key,
                self_id=self_id,
                lock_ttl_sec=lock_ttl_sec,
            )
        )
        self.situation = self.req_situation(situation.value)

        if situation == self.req_script_situation.lock_wait:
            signal_key = self.signal_key
            self._log("Subscribing to notify channel (lock_wait)")
            subscription = await self._wait_network_call(
                self.subscription_manager_cls.create(
                    cm_stack=cm_stack,
                    client=cli,
                    channel_key=signal_key,
                )
            )
            self._log("Re-checking the situation after subscription")
            # In case the result appeared between first `get` and `psubscribe`, check for it again.
            self.situation = self.req_situation.requesting
            situation, result = await self._wait_network_call(
                req_script(
                    lock_key=lock_key,
                    data_key=data_key,
                    self_id=self_id,
                    lock_ttl_sec=lock_ttl_sec,
                )
            )
            self.situation = self.req_situation(situation.value)
            if situation != self.req_script_situation.lock_wait:
                self._log(
                    "Situation changed while subscribing (to %r), unsubscribing",
                    situation,
                    situation=situation,
                )
                if subscription is not None:
                    # It is possible the `req_script` above has locked, and so
                    # the subscription would stay while data is being generated.
                    # This will try unsubscribing at this point, but it will
                    # also be awaited in the `cm_stack` finalization.
                    await self._finalize_maybe_in_background(
                        subscription.exit(),
                        name="subscription.exit() (situation changed)",
                    )
                subscription = None

        internal_situation = self.req_situation(situation.value)
        self._log(
            "Situation from get_data: %r",
            internal_situation,
            situation=internal_situation,
        )
        return internal_situation, result, subscription

    async def _wait_for_result(
        self,
        sub: SubscriptionManagerBase,
    ) -> Tuple[ReqResultInternal, Optional[bytes]]:
        # Lock should be renewed more often than `lock_ttl_sec`,
        # so waiting for the ttl duration should be sufficient.
        timeout = self.lock_ttl_sec
        data = None

        try:
            while True:
                self._log("Waiting for signal")
                self.situation = self.req_situation.awaiting
                message = await sub.get(timeout=timeout)

                if message is None:
                    self._log(
                        "Timed out waiting for signal, assuming the locking process is gone",
                        timeout=timeout,
                    )
                    situation = self.req_situation.lock_wait_timeout
                elif message.startswith(self.chan_data_prefix):
                    self._log("Data signal.")
                    data = message[len(self.chan_data_prefix) :]
                    situation = self.req_situation.cache_hit_after_wait
                elif message.startswith(self.chan_alive_prefix):
                    self._log("Alive signal: %r", message, timeout=timeout)
                    continue  # keep waiting
                elif message.startswith(self.chan_fail_prefix):
                    self._log("Fail signal: %r", message, timeout=timeout)
                    situation = self.req_situation.failure_signal
                else:
                    self._log("Unexpected signal message: %r", message, timeout=timeout)
                    situation = self.req_situation.lock_wait_unexpected_message

                self.situation = situation
                return situation, data

        finally:
            await sub.exit()

        raise Exception("Programming Error")

    async def _get_data_full(self) -> Tuple[ReqResultInternal, Optional[bytes]]:
        assert self._situation == self.req_situation.starting

        cli = self._client
        assert cli is not None, "must be initialized for this method"
        situation, result, subscription = await self._get_data()

        if situation == self.req_situation.lock_wait:
            # At this point, the `sub_client` should already be subscribed
            # without any race conditions.
            assert subscription is not None
            situation, result = await self._wait_for_result(sub=subscription)

        # Can fall through to either `cache_hit` or whichever
        # `self.channel_poll_timeout_situation` defines.
        return situation, result

    async def _renew_lock(self) -> int:
        self_id = self._self_id
        assert self_id is not None, "must be initialized for this method"
        cli = self._client
        assert cli is not None, "must be initialized for this method"

        lock_key = self.lock_key
        signal_key = self.signal_key
        lock_ttl_sec = self.lock_ttl_sec
        renew_script = self.renew_script_cls(cli=cli)
        self._log("Calling renew_script", lock_ttl_sec=lock_ttl_sec)
        return await self._wait_network_call(
            renew_script(
                lock_key=lock_key,
                signal_key=signal_key,
                self_id=self_id,
                lock_ttl_sec=lock_ttl_sec,
            )
        )

    async def _lock_pinger(self) -> None:
        cli = self._client
        assert cli is not None, "must be initialized for this method"

        lock_ttl_sec = self.lock_ttl_sec
        renew_interval = self.lock_renew_interval or lock_ttl_sec * 0.5
        while True:  # until cancelled, really
            await asyncio.sleep(renew_interval)
            try:
                renew_res = await self._renew_lock()
                # TODO: callback for non-ok renew.
                self._log(
                    "renew script result: %r", renew_res, renew_interval=renew_interval
                )
            except Exception as err:  # pylint: disable=broad-except
                self._log("lock_pinger error: %r", err, renew_interval=renew_interval)

    async def _save_data(
        self,
        data: bytes,
        ttl_sec: Optional[float] = None,
    ) -> Any:
        self_id = self._self_id
        assert self_id is not None, "must be initialized for this method"
        cli = self._client
        assert cli is not None, "must be initialized for this method"

        lock_key = self.lock_key
        signal_key = self.signal_key
        data_key = self.data_key
        self._log("Calling save_script", data_len=len(data))
        save_script = self.save_script_cls(cli=cli)
        # Not wrapping in `self._wait_network_call` at this point, as
        # cancelling this action is not appropriate.
        return await self._postpone_to_finalization(
            save_script(
                lock_key=lock_key,
                signal_key=signal_key,
                data_key=data_key,
                self_id=self_id,
                data=data,
                data_ttl_sec=ttl_sec or self.data_ttl_sec,
            )
        )

    async def _force_save_data(
        self,
        data: bytes,
        ttl_sec: Optional[float] = None,
    ) -> Any:
        cli = self._client
        assert cli is not None, "must be initialized for this method"

        signal_key = self.signal_key
        data_key = self.data_key
        self._log("Calling force_save_script", data_len=len(data))
        force_save_script = self.force_save_script_cls(cli=cli)
        return await self._postpone_to_finalization(
            force_save_script(
                signal_key=signal_key,
                data_key=data_key,
                data=data,
                data_ttl_sec=ttl_sec or self.data_ttl_sec,
            )
        )

    async def _save_failure(self, ignore_errors: bool = False) -> Any:
        self_id = self._self_id
        assert self_id is not None, "must be initialized for this method"
        cli = self._client
        assert cli is not None, "must be initialized for this method"

        lock_key = self.lock_key
        signal_key = self.signal_key
        self._log("Calling fail_script")
        fail_script = self.fail_script_cls(cli=cli)
        # TODO: save the script result to `self.situation`
        return await self._postpone_to_finalization(
            fail_script(
                lock_key=lock_key,
                signal_key=signal_key,
                self_id=self_id,
                ignore_errors=ignore_errors,
            )
        )

    async def _handle_initialize(
        self,
        situation: ReqResultInternal,
        result: Optional[bytes],
    ) -> Optional[bytes]:
        if situation in (
            self.req_situation.cache_hit,
            self.req_situation.cache_hit_after_wait,
        ):
            assert result is not None
            return result

        if situation == self.req_situation.successfully_locked:
            assert result is None

            self_id = self._self_id
            assert self_id is not None
            cm_stack = self._cm_stack
            assert cm_stack is not None

            await cm_stack.enter_async_context(task_cm(self._lock_pinger()))
            return None

        if situation in (
            self.req_situation.lock_wait_timeout,
            self.req_situation.failure_signal,
        ):
            assert result is None
            return None

        raise Exception(
            "RedisCacheLock error: Unexpected get_data_full outcome", situation
        )

    async def initialize(self) -> Optional[bytes]:
        """
        Attempt to read the data or lock the key or wait for data.

        Returns the cached (serialized) data, if available.

        After calling this function, `finalize` *must* be called in all cases.
        This pair essentially works as a context manager with different arguments.
        """
        assert self._situation is None, "not re-entrable"
        self.situation = self.req_situation.starting

        if self.enable_slave_get:
            result = await self.get_data_slave()
            if result is not None:
                self._log("Found data at slave")
                self.situation = self.req_situation.cache_hit_slave
                return result

        self_id = self.new_self_id()
        self._self_id = self_id

        cm_stack = AsyncExitStack()
        self._cm_stack = cm_stack  # `AsyncExitStack` allows aexit-without-aenter.

        await cm_stack.__aenter__()  # The corresponding `finally:` is the `def finalize`.

        cli: Redis = await cm_stack.enter_async_context(
            self._client_acm_managed(master=True)
        )
        self._client = cli

        situation, result = await self._get_data_full()
        self.situation = situation
        return await self._handle_initialize(
            situation=situation,
            result=result,
        )

    def decide_force_save(  # pylint: disable=unused-argument:
        self,
        situation: ReqResultInternal,
    ) -> bool:
        if situation in (
            # Will save the data 'nicely', i.e. only if the lock is not held by another process.
            self.req_situation.successfully_locked,
            # No data generated, probably shouldn't save it back.
            self.req_situation.cache_hit_slave,
            self.req_situation.cache_hit,
            self.req_situation.cache_hit_after_wait,
        ):
            return False

        # The safe-ish default: if something abnormal happened, but we
        # generated the data, might as well save it (although it can
        # technically be an error-marker).
        return True

    async def _handle_finalize(
        self,
        result: Optional[
            bytes
        ],  # must be specified but in some situations can be empty.
        ttl_sec: Optional[float] = None,
        force_save: Optional[bool] = None,
    ) -> Any:
        situation = self._situation

        if situation is None:
            # Failed before starting the initialization at all. Nothing to do.
            self.situation = self.req_situation.failure_to_initialize
            return

        if result is not None:
            if force_save is None:
                force_save = self.decide_force_save(situation)
            if force_save:
                self.situation = self.req_situation.generated_force_saved
                return await self._force_save_data(
                    data=result,
                    ttl_sec=ttl_sec,
                )

        if result is None and self._self_id is not None:
            # Initialization failure, data generation failure, etcetera.
            # Ensure we don't hold the lock, just in case.
            self.situation = self.req_situation.failure_marker_sent
            return await self._save_failure(ignore_errors=True)

        if situation == self.req_situation.successfully_locked:
            assert result is not None
            self.situation = self.req_situation.generated_saved
            return await self._save_data(data=result, ttl_sec=ttl_sec)

    async def finalize(
        self,
        result_serialized: Optional[bytes],
        ttl_sec: Optional[float] = None,
    ) -> None:
        try:
            return await self._handle_finalize(
                result=result_serialized,
                ttl_sec=ttl_sec,
            )
        finally:
            cm_stack = self._cm_stack
            self._cleanup()
            if cm_stack is not None:
                # Should not matter whether there was an exception.
                # The saving should happen in this finalization, because it
                # might be in background, but should be finished before the
                # client is released.
                await cm_stack.aclose()



    async def _call_generate_func(
        self, generate_func: TGenerateFunc
    ) -> TGenerateResult:
        """Call `generate_func` and validate the result"""
        result = await generate_func()
        if not isinstance(result, tuple):
            raise ValueError(
                (
                    "`generate_func` returned a non-tuple; "
                    "it should return a 2-item tuple `(serialized, unserialized)`"
                ),
                dict(generate_func=generate_func, result_type=type(result)),
            )
        if len(result) != 2:
            raise ValueError(
                (
                    f"`generate_func` returned a {len(result)}-item tuple; "
                    "it should return a 2-item tuple `(serialized, unserialized)`"
                ),
                dict(generate_func=generate_func),
            )
        serialized, raw = result
        if not isinstance(serialized, bytes):
            raise ValueError(
                "`generate_func` should return serialized value `bytes` as the first tuple item",
                dict(generate_func=generate_func, serialized_type=type(serialized)),
            )
        return serialized, raw

    # The code beyond this point can be used as a reference for calling
    # `initialize` + `finalize` directly.

    async def generate_with_lock(self, generate_func: TGenerateFunc) -> TCacheResult:
        result = None
        try:
            # General ordering note:
            # it is possible that the command reaches the server and gets executed,
            # but the code raises an exception (e.g. asyncio timeout). For that
            # reason, it is better to free the resources even if the acquiring
            # failed, and do it in a shielded background task.
            result = await self.initialize()
            if result is not None:
                return result, None  # cached

            result, unserialized_result = await self._call_generate_func(generate_func)
            return result, unserialized_result
        finally:
            await self.finalize(result)
