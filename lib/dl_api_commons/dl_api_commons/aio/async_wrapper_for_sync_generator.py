from __future__ import annotations

import abc
import asyncio
from asyncio import AbstractEventLoop
from concurrent.futures.thread import ThreadPoolExecutor
import enum
import logging
import threading
from typing import (
    Generator,
    Generic,
    Optional,
    TypeVar,
    Union,
)

import attr


try:
    from asyncio.exceptions import TimeoutError
except ImportError:
    from asyncio.futures import TimeoutError  # type: ignore  # TODO: fix


class JobState(enum.Enum):
    worker_not_started = enum.auto()  # Initial state
    worker_startup_confirmed = enum.auto()  # Control sets this to confirm that worker should perform initialization
    worker_ready = enum.auto()  # Worker sets this state to indicate that generator is instantiated and
    ready_for_next_chunk = enum.auto()
    chunk_in_buffer = enum.auto()
    end_of_stream = enum.auto()
    pending_close = enum.auto()
    closed = enum.auto()
    error = enum.auto()


class _ErrorInWorkerThread(Exception):
    """
    Exception is thrown by sync control methods to indicate that exception fires in worker.
    Desired action in control coroutines: await worker thread future to re-raise exception.
    """


class _WaitForStateTimeout(Exception):
    """Indicates that target state was not reached within timeout"""


class AWFSGExeption(Exception):
    """Base exception"""


class InitializationFailed(AWFSGExeption):
    """
    Fires in case of errors during worker initialization phase:
      sync generator creation fails
      worker thread was not acquired within timeout
    """


class AlreadyStarted(AWFSGExeption):
    pass


class WorkerIsClosed(AWFSGExeption):
    pass


class WorkerDoesNotThrowExceptionInErrorState(AWFSGExeption):
    pass


class EndOfStream(AWFSGExeption):
    pass


class AWFSGRuntimeError(AWFSGExeption):
    """Something strange and totally unexpected happens"""

    pass


_STATE_ITEM_TV = TypeVar("_STATE_ITEM_TV")


class _NoSet:
    instance: "_NoSet" = None  # type: ignore  # TODO: fix


_NoSet.instance = _NoSet()


@attr.s
class SynchronizedJobState(Generic[_STATE_ITEM_TV]):
    _log: logging.LoggerAdapter = attr.ib()
    _monitor: threading.Condition = attr.ib(init=False, factory=threading.Condition)
    _buffer: Union[_NoSet, _STATE_ITEM_TV] = attr.ib(init=False, default=_NoSet.instance)
    _state: JobState = attr.ib(init=False, default=JobState.worker_not_started)

    def _ensure_monitor(self):  # type: ignore  # TODO: fix
        if not self._monitor._is_owned():  # type: ignore  # TODO: fix  # noqa
            msg = "Attempt to use synchronized method without monitor acquiring"
            self._log.error(msg)
            raise AWFSGRuntimeError(msg)

    def fetch_buffer(self) -> _STATE_ITEM_TV:
        self._ensure_monitor()
        if self._buffer is _NoSet.instance:
            msg = "Trying to fetch from empty buffer"
            self._log.error(msg)
            raise AWFSGRuntimeError(msg)
        self._buffer, ret = _NoSet.instance, self._buffer
        return ret  # type: ignore  # TODO: fix

    def set_buffer(self, val: _STATE_ITEM_TV):  # type: ignore  # TODO: fix
        self._ensure_monitor()
        if self._buffer is not _NoSet.instance:
            msg = "Trying to reset buffer"
            self._log.error(msg)
            raise AWFSGRuntimeError(msg)
        self._buffer = val

    def set_state(self, state: JobState):  # type: ignore  # TODO: fix
        self._ensure_monitor()
        self._state = state
        self._monitor.notify_all()

    @property
    def state(self) -> JobState:
        return self._state

    def wait_for_state_change(self, state: Optional[JobState] = None, timeout: Optional[float] = None) -> None:
        self._ensure_monitor()
        target_state = state
        state_before = self._state
        if state is None:
            self._log.debug("Waiting for state monitor event")
            self._monitor.wait(timeout=timeout)
        else:
            self._log.debug("Waiting for particular state %s for %s", target_state, timeout)
            result = self._monitor.wait_for(lambda: self._state == target_state, timeout=timeout)
            if not result:
                self._log.debug("Requested state was not achieved within timeout %s", timeout)
                raise _WaitForStateTimeout()
        state_after = self._state
        self._log.debug("State monitor event received. Transition: %s -> %s", state_before, state_after)

    def __enter__(self):  # type: ignore  # TODO: fix
        return self._monitor.__enter__()

    def __exit__(self, *args, **kwargs):  # type: ignore  # TODO: fix
        return self._monitor.__exit__(*args, **kwargs)  # type: ignore  # TODO: fix


_JOB_ITEM_TV = TypeVar("_JOB_ITEM_TV")


# TODO Switch level to debug
@attr.s(cmp=False, hash=False)
class Job(Generic[_JOB_ITEM_TV], metaclass=abc.ABCMeta):
    _service_tpe: ThreadPoolExecutor = attr.ib()
    _workers_tpe: ThreadPoolExecutor = attr.ib()
    _worker_thread_start_timeout: float = attr.ib(default=0.1)
    _worker_thread_start_confirmation_timeout: float = attr.ib(default=0.1)

    _loop: AbstractEventLoop = attr.ib(init=False, factory=asyncio.get_event_loop)
    _worker_done_fut: Optional[asyncio.Future] = attr.ib(init=False, default=None)

    _ss: SynchronizedJobState = attr.ib(init=False, default=None)
    _startup_lock: asyncio.Lock = attr.ib(factory=asyncio.Lock)
    _worker_thread_started_event = attr.ib(init=False, factory=asyncio.Event)
    _log: logging.LoggerAdapter = attr.ib(init=False, default=None)

    def __attrs_post_init__(self):  # type: ignore  # TODO: fix
        self._log = logging.LoggerAdapter(
            logging.getLogger(__name__),
            extra=dict(
                job_sys_id=id(self),
            ),
        )
        self._ss = SynchronizedJobState(log=self._log)

    @property
    def state(self) -> JobState:
        return self._ss.state

    @abc.abstractmethod
    def make_generator(self) -> Generator[_JOB_ITEM_TV, None, None]:
        pass

    def _worker_do_step(self, generator: Generator) -> bool:
        """:returns True if there is work to handle in next step"""
        if self.state == JobState.ready_for_next_chunk:
            try:
                fetched_chunk = next(generator)

            except StopIteration:
                self._ss.set_state(JobState.end_of_stream)
                return False

            except Exception:
                self._ss.set_state(JobState.error)
                raise

            else:
                self._ss.set_buffer(fetched_chunk)
                self._ss.set_state(JobState.chunk_in_buffer)

        elif self.state == JobState.pending_close:
            # Notifying generator that we won't receive any more data
            try:
                generator.close()
            except Exception:  # noqa
                self._log.exception("Exception during generator closing")

            self._ss.set_state(JobState.closed)
            return False

        else:
            self._log.error("Unknown state value, stopping processing: %s", self.state)
            self._ss.set_state(JobState.error)
            return False

        self._log.debug("Waiting for job state monitor")
        self._ss.wait_for_state_change()
        return True

    def _worker_thread_target(self) -> None:
        self._loop.call_soon_threadsafe(self._worker_thread_started_event.set)
        with self._ss:
            self._log.debug("Waiting for 'start_confirmed' state")
            try:
                self._ss.wait_for_state_change(
                    state=JobState.worker_startup_confirmed, timeout=self._worker_thread_start_confirmation_timeout
                )
            except _WaitForStateTimeout:
                self._log.error(
                    "Worker thread did not receive start confirmation within timeout %s",
                    self._worker_thread_start_confirmation_timeout,
                )
                self._ss.set_state(JobState.error)
                return

            self._log.debug("Start was confirmed. Trying to create generator")

            try:
                generator = self.make_generator()
            except Exception:
                self._log.debug("Generator creation was failed. Setting state 'error'")
                self._ss.set_state(JobState.error)
                raise

            self._ss.set_state(JobState.worker_ready)
            # Here worker thread is totally ready for processing items in
            self._ss.wait_for_state_change()

            try:
                should_continue = True
                while should_continue:
                    should_continue = self._worker_do_step(generator)
            finally:
                if generator.gi_frame is not None:
                    self._log.error("Worker main cycle exits with running generator")
                    try:
                        generator.close()
                    except Exception:  # noqa
                        self._log.exception("Exception during generator closing")

    # Initialization
    def _sync_confirm_worker_start_set_ready_for_data(self) -> None:
        with self._ss:
            self._ss.set_state(JobState.worker_startup_confirmed)
            self._ss.wait_for_state_change()

            if self.state == JobState.worker_ready:
                self._ss.set_state(JobState.ready_for_next_chunk)
            elif self.state == JobState.error:
                self._ss.set_state(JobState.closed)
                raise _ErrorInWorkerThread
            else:
                self._log.error("Unexpected state during startup procedure: %s", self.state)
                self._ss.set_state(JobState.pending_close)

    async def run(self) -> None:
        """Start worker in configured TPE and ensure it will start in configured timeout."""
        self._log.info("Starting async wrapper for sync generator")
        async with self._startup_lock:
            if self.state == JobState.worker_not_started:
                self._log.debug("Scheduling worker thread")
                self._worker_done_fut = asyncio.ensure_future(
                    self._loop.run_in_executor(self._workers_tpe, self._worker_thread_target)
                )
                self._log.debug("Waiting for 'worker_thread_started_event' from worker thread")
                try:
                    await asyncio.wait_for(
                        self._worker_thread_started_event.wait(),
                        timeout=self._worker_thread_start_timeout,
                    )
                # We don't need to close worker thread in case of timeout:
                #  Worker will wait for 'start_confirmed' state with timeout
                #  If timeout fires - it will terminate
                except TimeoutError as err:
                    raise InitializationFailed(
                        f"Worker thread did not start in {self._worker_thread_start_timeout}"
                    ) from err
                except Exception as err:
                    raise InitializationFailed() from err

                self._log.debug("Event 'worker_thread_started_event' received")
                try:
                    await self._loop.run_in_executor(
                        self._service_tpe, self._sync_confirm_worker_start_set_ready_for_data
                    )
                except _ErrorInWorkerThread:
                    try:
                        self._log.info("Worker state was switched to error. Awaiting worker thread...")
                        await self._worker_done_fut
                    except Exception:
                        self._log.info("Exception from worker thread was caught", exc_info=True)
                        raise
                    else:
                        raise WorkerDoesNotThrowExceptionInErrorState()
            else:
                raise AWFSGRuntimeError("Job already started")

    # Fetching
    def _sync_fetch_chunk_from_buffer_schedule_next(self) -> _JOB_ITEM_TV:
        self._log.debug("Waiting for state monitor to proceed chunk fetch")
        with self._ss:
            self._log.debug("State monitor acquired to proceed chunk fetch")
            while True:
                if self.state == JobState.end_of_stream:
                    self._ss.set_state(JobState.closed)
                    raise EndOfStream()

                elif self.state == JobState.chunk_in_buffer:
                    ret = self._ss.fetch_buffer()
                    self._ss.set_state(JobState.ready_for_next_chunk)
                    return ret

                elif self.state == JobState.ready_for_next_chunk:
                    # Case: worker thread did not catch monitor after previous fetch procedure launch
                    #  So we should give it another chance to
                    self._ss.wait_for_state_change()
                    continue

                elif self.state == JobState.error:
                    self._ss.set_state(JobState.closed)
                    raise _ErrorInWorkerThread()

                elif self.state in (JobState.closed, JobState.pending_close):
                    raise WorkerIsClosed()

                else:
                    self._log.error(
                        "Unexpected state encountered during fetching next, scheduling close: %s", self.state
                    )
                    self._ss.set_state(JobState.pending_close)
                    raise AWFSGRuntimeError(f"Unexpected state encountered during fetching next: {self.state}")

    # TODO FIX: Use async lock to prevent concurrent calls
    # TODO FIX: Assert that job is running
    async def get_next(self) -> _JOB_ITEM_TV:
        try:
            next_chunk = await self._loop.run_in_executor(
                self._service_tpe, self._sync_fetch_chunk_from_buffer_schedule_next
            )
            return next_chunk
        except _ErrorInWorkerThread:
            try:
                self._log.info("Worker state was switched to error. Awaiting worker thread...")
                await self._worker_done_fut  # type: ignore  # TODO: fix
            except Exception:
                self._log.info("Exception from worker thread was caught", exc_info=True)
                raise
            else:
                raise WorkerDoesNotThrowExceptionInErrorState()

    # Cancellation
    def _sync_close_wait_closed(self) -> None:
        self._log.info("Awaiting monitor to make state transition to closing")
        with self._ss:
            if self.state in (JobState.ready_for_next_chunk, JobState.chunk_in_buffer, JobState.pending_close):
                if self.state != JobState.pending_close:
                    self._log.info("Settings state to closing")
                    self._ss.set_state(JobState.pending_close)
                else:
                    self._log.info("State already is 'pending_close'")
                # TODO FIX: May be timeout
                while True:
                    self._log.info("Waiting for worker to stop")
                    self._ss.wait_for_state_change()
                    if self.state.closed:
                        self._log.info("State 'closed' was reached")
                        return

            elif self.state == JobState.closed:
                self._log.info("Already closed")
                return

            elif self.state == JobState.end_of_stream:
                self._ss.set_state(JobState.closed)
                return

            elif self.state == JobState.error:
                self._ss.set_state(JobState.closed)
                raise _ErrorInWorkerThread()

            elif self.state == JobState.worker_not_started:
                return

            else:
                raise AWFSGRuntimeError(f"Unexpected state during close: {self.state}")

    # TODO FIX: Use async lock to prevent concurrent calls
    # TODO FIX: Assert that job is running
    async def cancel(self) -> None:
        self._log.info("Launching close procedure in service TPE")
        try:
            await self._loop.run_in_executor(self._service_tpe, self._sync_close_wait_closed)
        except _ErrorInWorkerThread:
            await self._worker_done_fut  # type: ignore  # TODO: fix
        self._log.info("Generator was successfully cancelled")
