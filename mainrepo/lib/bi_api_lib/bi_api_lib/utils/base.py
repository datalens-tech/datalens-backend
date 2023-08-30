""" ... """

from __future__ import annotations

import cProfile
import datetime
import logging
import os
import sys
import uuid
from contextlib import asynccontextmanager, contextmanager, AbstractContextManager
from typing import TYPE_CHECKING, Any, AsyncIterator, Dict, Iterable, Iterator, List, Optional, Tuple, cast

# noinspection PyUnresolvedReferences

from bi_app_tools.profiling_base import GenericProfiler
from bi_core.flask_utils.us_manager_middleware import USManagerFlaskMiddleware
import bi_core.exc as common_exc
from bi_connector_chyt_internal.core.us_connection import BaseConnectionCHYTInternal
from bi_utils.sanitize import clear_hash

from bi_api_lib.enums import USPermissionKind

if TYPE_CHECKING:
    from bi_query_processing.execution.exec_info import QueryExecutionInfo
    from bi_core.us_entry import USEntry

LOGGER = logging.getLogger(__name__)


@contextmanager
def query_execution_context(
        log_error: bool = True,
        dataset_id: Optional[str] = None,
        version: Optional[str] = None,
        body: Optional[dict] = None,
) -> Iterator[None]:
    try:
        yield  # execute query here
    except common_exc.DatabaseQueryError as err:
        if log_error:
            LOGGER.info(
                'Failed SQL query for dataset %s-%s. Request: %s.  Query: %s. Error: %s',
                dataset_id, version, str(body)[:1000], err.query, err.db_message,
                exc_info=True,
            )
        raise


@contextmanager
def profile_stats(stats_dir: str = None) -> Iterator[None]:
    """Save profiler stats to file"""
    stats_dir = stats_dir or './cprofiler'
    if not os.path.exists(stats_dir):
        os.makedirs(stats_dir)
    pr = cProfile.Profile()
    try:
        pr.enable()
        yield
    finally:
        pr.disable()
        filename = os.path.join(
            stats_dir,
            '{}-{}.stats'.format(
                datetime.datetime.utcnow().strftime('%Y-%m-%d-%H-%M-%S'),
                str(uuid.uuid4())[:4]))
        pr.dump_stats(filename)


def need_permission(us_entry_id: str, permission: USPermissionKind) -> None:
    usm_user = USManagerFlaskMiddleware.get_request_us_manager()
    entry = usm_user.get_by_id(us_entry_id)
    need_permission_on_entry(entry, permission)


def need_permission_on_entry(us_entry: USEntry, permission: USPermissionKind) -> None:
    assert us_entry.permissions is not None
    assert us_entry.uuid is not None
    if not us_entry.permissions[permission.name]:
        raise common_exc.USPermissionRequired(us_entry.uuid, permission.name)


def chunks(lst: List[Any], size: int) -> Iterable[List[Any]]:
    """ Yield successive chunks from lst. No padding.  """
    for idx in range(0, len(lst), size):
        yield lst[idx:idx + size]


def split_by_quoted_quote(value: str, quote: str = "'") -> Tuple[str, str]:
    """
    Parse out a quoted value at the beginning,
    where quotes are quoted by doubling (CSV-like).

    >>> split_by_quoted_quote("'abc'de")
    ('abc', 'de')
    >>> split_by_quoted_quote("'ab''c'''de")
    ("ab'c'", 'de')
    >>> split_by_quoted_quote("'ab''c'''")
    ("ab'c'", '')
    """
    ql = len(quote)
    if not value.startswith(quote):
        raise ValueError("Value does not start with quote")
    value = value[ql:]
    result = []
    while True:
        try:
            next_quote = value.index(quote)
        except ValueError:
            raise ValueError("Unclosed quote")
        value_piece = value[:next_quote]
        result.append(value_piece)
        value = value[next_quote + ql:]
        if value.startswith(quote):
            result.append(quote)
            value = value[ql:]
        else:  # some other text, or end-of-line.
            break

    return ''.join(result), value


def quote_by_quote(value: str, quote: str = "'") -> str:
    """
    ...

    >>> quote_by_quote("a'b'")
    "'a''b'''"
    >>> split_by_quoted_quote(quote_by_quote("a'b'") + "and 'stuff'")
    ("a'b'", "and 'stuff'")
    """
    return "{}{}{}".format(
        quote,
        value.replace(quote, quote + quote),
        quote)


class SemaphoreEmpty(Exception):
    """ No free slot in a semaphore (see: `.flock_semaphore`) """


@contextmanager
def flock_semaphore(path_tpl: str, size: int, extra_data: str = '') -> Iterator[Dict]:
    """
    `flock`-based semaphore.
    Uses numbered files as the backend.
    Gets unlocked if the process is killed.

    Example arguments: `flock_semaphore(base='/tmp/semtst_%02d.lock', size=3)`

    Shell reference for getting the count of available semaphores:

        sem_value() { for fln in "$@"; do if flock --nonblock "$fln" true; then printf "%s\n" "$fln"; fi; done; }; sem_value /tmp/.bi_*.lock | wc -l

    """
    import fcntl
    paths = (path_tpl % (idx + 1,) for idx in range(size))
    for path in paths:
        fd = os.open(path, os.O_CREAT | os.O_WRONLY)
        try:

            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                continue  # not available, try the next

            # Locked successfully.
            os.write(fd, f'{os.getpid()}\n{extra_data}\n'.encode('utf-8'))
            try:
                yield dict(path=path)
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.write(fd, '.\n'.encode('utf-8'))
            return

        finally:
            os.close(fd)

    raise SemaphoreEmpty()


@contextmanager
def worker_control_cm(exec_info: 'QueryExecutionInfo') -> Iterator[Optional[Dict]]:
    size_from_env = os.environ.get('BI_WORKERS_YT_SEMAPHORE_SIZE')
    if not size_from_env:
        LOGGER.debug("worker_control_cm: not enabled by `BI_WORKERS_YT_SEMAPHORE_SIZE`.")
        yield None
        return

    size = int(size_from_env)

    target_connections = exec_info.target_connections
    assert len(target_connections) == 1
    conn = target_connections[0]

    lock_name = None
    cluster = None
    alias = None
    if isinstance(conn, BaseConnectionCHYTInternal):
        cluster = conn.data.cluster
        alias = conn.data.alias or ''
        alias_clear = clear_hash(alias)
        # For public-only, use `if alias.replace('*', '') in ('ch_datalens',):`
        lock_name = f'bi_workers_chyt__{cluster}__{alias_clear}'

    if lock_name is None:
        LOGGER.debug("worker_control_cm: not a relevant connection: %r.", conn.__class__)
        yield None
        return

    path_tpl = f'/tmp/.{lock_name}_%02d.lock'
    sem = flock_semaphore(path_tpl=path_tpl, size=size)
    LOGGER.debug("Acquiring a semaphore at %r...", path_tpl)
    try:
        with sem as sem_res:
            LOGGER.debug("Acquired a semaphore: %r;", sem_res)
            yield dict(sem=sem, sem_res=sem_res)
    except SemaphoreEmpty:
        LOGGER.warning("worker_control_cm: SemaphoreEmpty at %r;", path_tpl)
        raise common_exc.DatabaseUnavailable(
            message=(
                f'Too many queries on {cluster} {alias!r},'
                f' new queries cannot be processed at the moment.'))


@asynccontextmanager
async def worker_control_cm_async(exec_info: 'QueryExecutionInfo') -> AsyncIterator[None]:
    sync_cm = cast(AbstractContextManager, worker_control_cm(exec_info))

    with GenericProfiler('wccm_enter'):
        sync_cm.__enter__()

    try:
        yield
    finally:
        sync_cm.__exit__(*sys.exc_info())
