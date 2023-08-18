""" https://wiki.yandex-team.ru/users/hhell/tasksoverpg/ """
from __future__ import annotations

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_pg

import os
import socket
import random
import logging
# import multiprocessing
import threading
from datetime import timedelta, datetime as dt
from _thread import interrupt_main as interrupt_main_thread
from .utils import datetime_now, UTC
from . import db
from . import db_utils


def make_lock_string():
    return "{host}__{pid}__{rnd:04x}".format(
        host=socket.gethostname(),
        pid=os.getpid(),
        rnd=random.getrandbits(16))


class DBEngineRetryWrap:
    """
    Does a single `engine.execute` call retries, under the assumptions that no
    transactions are involved and all queries are idempotent.
    """

    _max_tries = 3

    def __init__(self, db_engine):
        self._db_engine = db_engine
        self._logger = logging.getLogger(self.__class__.__name__)

    def __getattr__(self, key):
        return getattr(self._db_engine, key)

    def _can_retry_error(self, exc):
        return db_utils.can_retry_error(exc)

    def execute(self, *args, **kwargs):
        db_engine = self._db_engine
        logger = self._logger
        max_tries = self._max_tries
        wrapped_func = db_engine.execute
        for retries_remain in reversed(range(max_tries)):
            try:
                return wrapped_func(*args, **kwargs)
            except Exception as exc:  # pylint: disable=broad-except
                if not retries_remain:
                    raise
                if not self._can_retry_error(exc):
                    logger.exception("Unretriable: %r", exc)
                    raise
                setattr(exc, '_already_retried', True)
                logger.warning("db execute error (retriable %r): %r", retries_remain, exc)
        raise Exception("Programming Error")


class TasksManager:

    _min_pause = 1.0

    def __init__(
            self, task_functions, db_engine, db_table=db.PeriodicTask, ensured_tasks=(),
            # pool_size=5,
    ):
        self.task_functions = task_functions
        self.ensured_tasks = ensured_tasks
        self.db_engine = DBEngineRetryWrap(db_engine)
        self.db_session = db.get_session(engine=db_engine)
        self.db_table = getattr(db_table, '__table__', db_table)
        self.wakeup_event = threading.Event()
        self.running = False
        self.logger = logging.getLogger(self.__class__.__name__)
        # self.pool_size = pool_size
        # self.executor_pool = multiprocessing.Pool(pool_size)

    def prepare(self):
        self._ensure_tasks()

    def run(self):
        self.prepare()
        self.running = True
        while self.running:
            sleep_time = self._process_once()
            self.logger.debug("Sleeping for %r", sleep_time)
            self.wakeup_event.wait(sleep_time)

    def _process_once(self, now=None):
        if now is None:
            now = datetime_now()

        tasks = self._get_all_tasks()
        self.logger.debug("Processing %d tasks", len(tasks))
        due, pending, next_time = self._process_scheduling(tasks, now=now)
        if not due:
            self.logger.info("Nothing to start")
        for task in due:
            self._run_due_task(task)
        if next_time is None:
            assert not tasks
            self.logger.warning("No tasks to wait for")
            time_to_next_task = self._min_pause * 60
        else:
            time_to_next_task = (next_time - datetime_now()).total_seconds()
            if time_to_next_task < self._min_pause:
                self.logger.warning("time to next task is too low: %r", time_to_next_task)
                time_to_next_task = self._min_pause
        return time_to_next_task

    def stop(self):
        assert self.running
        self.running = False
        self.wakeup_event.set()

    def _ensure_tasks(self):
        objs = self.ensured_tasks
        if not objs:
            return
        stmt = sa_pg.insert(self.db_table).values(objs).on_conflict_do_nothing()
        self.db_engine.execute(stmt)

    def _get_all_tasks(self):
        return list(self.db_engine.execute(self.db_table.select()))
        # if self.ensured_tasks:
        #     result_names = set(task.name for task in result)
        #     result += list(task for task in self.ensured_tasks if task.name not in result_names)
        # return result

    def _process_scheduling(self, tasks, now=None):
        """ tasks -> due, pending, time_of_the_next_event """
        if now is None:
            now = datetime_now()

        due = []
        pending = []
        min_time = None

        for task in tasks:
            locked = False
            lock_expire_ts = None
            if task.lock and task.last_ping_ts:
                # self.logger.debug("%s is maybe locked", task.name)
                lock_expire_ts = task.last_ping_ts + timedelta(seconds=task.lock_expire)
                locked = lock_expire_ts > now

            minimal_dt = UTC.localize(dt.min)  # pylint: disable=no-value-for-parameter
            last_start = task.last_start_ts or minimal_dt
            # self.logger.debug("%s last_start=%r, relative=%.3f", task.name, last_start, (now - last_start).total_seconds())
            next_start = last_start + timedelta(seconds=task.frequency)
            if locked:
                # Tricky optimization point:
                # If task is locked but is due to be run soon (in the future),
                # try it again at the point it is supposed to be run at.
                # However, if it is overdue to be run (the normal run point is
                # in the past), avoid poking the task too often and wait until
                # the time the lock should expire.
                # self.logger.debug("%s is locked; next_start=%r, lock_expire_ts=%r", task.name, next_start, lock_expire_ts)
                if next_start < now:
                    next_start = max(next_start, lock_expire_ts)  # type: ignore  # TODO: fix

            if next_start <= now:
                # self.logger.debug("%s is due", task.name)
                due.append(task)
                next_start += timedelta(seconds=task.frequency)
                if next_start < now:
                    _steps = int((now - next_start).total_seconds() / task.frequency) + 1
                    # self.logger.info("Skipping %r runs", _steps)
                    next_start += timedelta(seconds=_steps * task.frequency)
            else:
                # self.logger.debug("%s is pending", task.name)
                pending.append(task)

            min_time = min(min_time, next_start) if min_time else next_start  # type: ignore  # TODO: fix

        return due, pending, min_time

    def _run_due_task(self, task, by_fork=True):
        try:
            func = self.task_functions[task.name]
        except KeyError:
            self.logger.error("Unknown task: %r (%r); known tasks: %r", task.name, task, list(self.task_functions.keys()))
            return

        self.logger.info("Running task: %r (%r)", task.name, task)

        call_func = self._task_wrapper
        call_kwds = dict(
            func=func, name=task.name, lock_renew=task.lock_renew,
            task_obj=task,
        )

        if by_fork:
            newpid = os.fork()
            if newpid:
                self.logger.debug("Forked off: %r", newpid)
                self.db_engine.dispose()
                return

        try:
            call_func(**call_kwds)
        finally:
            if by_fork:
                os._exit(0)

    #     self.executor_pool.apply_async(
    #         call_func, kwds=call_kwds,
    #         callback=functools.partial(self._executor_result, task.name),
    #         error_callback=functools.partial(self._executor_error, task.name),
    #     )

    # def _executor_result(self, name, *args, **kwargs):
    #     self.logger.debug("Done: %r; %r, %r", name, args, kwargs)

    # def _executor_error(self, name, *args, **kwargs):
    #     self.logger.error("Failed: %r; %r, %r", name, args, kwargs)

    def _on_after_fork(self, logger):
        # Make sure at least some of the db connections aren't used after a
        # fork().
        # https://docs.sqlalchemy.org/en/latest/faq/connections.html?highlight=fork#how-do-i-use-engines-connections-sessions-with-python-multiprocessing-or-os-fork
        logger.debug("Clearing the db connections (assuming after-fork)")
        self.db_engine.dispose()

    def _task_wrapper(self, func, name, lock_renew, task_obj=None, fork_handler=True):
        logger = self.logger.getChild('task.{}'.format(name))
        logger.info("Starting")

        if fork_handler:
            self._on_after_fork(logger=logger)

        db_engine = self.db_engine
        locker = TaskLocker(
            name=name, lock_renew=lock_renew,
            db_engine=db_engine, db_table=self.db_table,
            # logger=logger.getChild('locker'),
        )
        try:
            with locker:
                result = func(name=name, task_obj=task_obj, locker=locker, manager=self, fork_handler=fork_handler)
            self.logger.info("Done")
            self.logger.debug("... result=%r", result)
        except TaskLocker.LockNotAcquired as exc:
            self.logger.info("Not locked: %r", exc)
        except (TaskLocker.LockExpired, KeyboardInterrupt) as exc:
            self.logger.error("Lock expired / interrupted: %r", exc)
        except Exception as exc:
            self.logger.exception("func raised %r", exc)
            raise


class TaskLocker:

    class LockError(Exception):
        """ Could not (re)lock """

    class LockNotAcquired(LockError):
        """ Locked by another process, presumably. """

    class LockExpired(LockError):
        """
        Could not ensure the lock is fresh.
        `KeyboardInterrupt` might also happen instead of this exception.
        """

    def __init__(self, name, lock_renew, db_engine, db_table, lock_string=None, join_timeout=1, logger=None):
        self.name = name
        self.logger = logger or logging.getLogger(self.__class__.__name__).getChild(name)
        self.db_engine = db_engine
        self.db_table = getattr(db_table, '__table__', db_table)
        self.lock_string = lock_string or make_lock_string()
        self.lock_renew = lock_renew
        self.pinger_stopper = threading.Event()
        self.join_timeout = join_timeout
        self.pinger_thread = threading.Thread(target=self.pinger)
        self.pinger_thread.daemon = True

    def __enter__(self):
        self.logger.debug("Starting...")
        self.set_lock(is_starting=True)
        self.pinger_thread.start()
        self.logger.debug("Starting done.")
        return self

    def __exit__(self, *ei):
        now = datetime_now()
        is_failure = ei[0] is not None
        self.logger.debug("Stopping (is_failure=%r)...", is_failure)
        self.pinger_stopper.set()
        self.pinger_thread.join(self.join_timeout)
        unset_res = self.unset_lock(is_failure=is_failure, now=now)
        self.logger.debug("Stopping done (is_failure=%r, unset_res=%r)", is_failure, unset_res)

    def pinger(self):
        while not self.pinger_stopper.is_set():
            try:
                self.set_lock()
            except self.LockError:
                self.interrupt_parent()
                raise
            self.logger.debug("Waiting for %r", self.lock_renew)
            self.pinger_stopper.wait(self.lock_renew)

    def interrupt_parent(self):
        # NOTE: can't exactly easily interrupt *parent* thread, alas.
        return interrupt_main_thread()

    def _update_stmt(self, allow_unlocked=False, now=None):
        if now is None:
            now = datetime_now()
        tbl = self.db_table
        stmt = tbl.update().where(tbl.c.name == self.name)
        lock_filter = (tbl.c.lock == self.lock_string)
        if allow_unlocked:
            lock_filter = lock_filter | (
                tbl.c.lock == None  # noqa: E711  # pylint: disable=singleton-comparison
            )
            # ... or expired
            pg_second = sa.cast('1 second', sa_pg.INTERVAL)
            pg_minimal_ts = sa.cast("'-infinity'", sa_pg.TIMESTAMP)
            last_ping_ts_or_min = sa.sql.functions.coalesce(tbl.c.last_ping_ts, pg_minimal_ts)
            lock_filter = lock_filter | (
                last_ping_ts_or_min + tbl.c.lock_expire * pg_second < now)
        stmt = stmt.where(lock_filter)
        return stmt

    def _current_lock_info(self):
        tbl = self.db_table
        result = list(self.db_engine.execute(
            tbl.select().where(tbl.c.name == self.name)
        ))
        if len(result) == 1:
            result = dict(result[0])  # type: ignore  # TODO: fix
        return result

    def set_lock(self, is_starting=False):
        if is_starting:
            self.logger.debug("set_lock(is_starting=True)")
        else:
            self.logger.debug("set_lock()")

        # TODO: try several times, for reliability
        now = datetime_now()
        # tbl = self.db_table

        extra_upd = {}
        if is_starting:
            extra_upd['last_start_ts'] = now

        ping_meta = dict(
            host=socket.gethostname(),
            pid=os.getpid(),
            now=now.isoformat(),
        )

        # NOTE/UNCERTAIN: for lock renewal, disallow 'unlocked' as a valid state.
        stmt = self._update_stmt(allow_unlocked=is_starting, now=now)
        stmt = stmt.values(
            lock=self.lock_string, last_ping_ts=now,
            last_ping_meta=ping_meta, **extra_upd)
        result = self.db_engine.execute(stmt)
        if result.rowcount != 1:
            info = self._current_lock_info()
            exc_cls = self.LockNotAcquired if is_starting else self.LockExpired
            raise exc_cls("Lock update modified %r rows; after-the-fact state: %r" % (
                result.rowcount, info))

    def unset_lock(self, is_failure, now=None):
        if now is None:
            now = datetime_now()

        extra_upd = {}
        if is_failure:
            extra_upd['last_failure_ts'] = now
        else:
            extra_upd['last_success_ts'] = now

        stmt = self._update_stmt(allow_unlocked=False, now=now)
        stmt = stmt.values(lock=None, **extra_upd)
        result = self.db_engine.execute(stmt)
        if result.rowcount != 1:
            self.logger.error("Failed to properly unlock, somehow")
            self.logger.debug("Lock info: %r", self._current_lock_info())
            return False
        return True
