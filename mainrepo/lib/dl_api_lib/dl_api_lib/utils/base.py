""" ... """

from __future__ import annotations

import cProfile
from contextlib import contextmanager
import datetime
import logging
import os
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Iterator,
    Optional,
)
import uuid

from dl_api_lib.enums import USPermissionKind
from dl_app_tools.profiling_base import GenericProfiler
import dl_core.exc as common_exc
from dl_core.flask_utils.us_manager_middleware import USManagerFlaskMiddleware


# noinspection PyUnresolvedReferences


if TYPE_CHECKING:
    from dl_core.us_entry import USEntry

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
                "Failed SQL query for dataset %s-%s. Request: %s.  Query: %s. Error: %s",
                dataset_id,
                version,
                str(body)[:1000],
                err.query,
                err.db_message,
                exc_info=True,
            )
        raise


@contextmanager
def profile_stats(stats_dir: Optional[str] = None) -> Iterator[None]:
    """Save profiler stats to file"""
    stats_dir = stats_dir or "./cprofiler"
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
            "{}-{}.stats".format(datetime.datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S"), str(uuid.uuid4())[:4]),
        )
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


def chunks(lst: list[Any], size: int) -> Iterable[list[Any]]:
    """Yield successive chunks from lst. No padding."""
    for idx in range(0, len(lst), size):
        yield lst[idx : idx + size]


def split_by_quoted_quote(value: str, quote: str = "'") -> tuple[str, str]:
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
        value = value[next_quote + ql :]
        if value.startswith(quote):
            result.append(quote)
            value = value[ql:]
        else:  # some other text, or end-of-line.
            break

    return "".join(result), value


def quote_by_quote(value: str, quote: str = "'") -> str:
    """
    ...

    >>> quote_by_quote("a'b'")
    "'a''b'''"
    >>> split_by_quoted_quote(quote_by_quote("a'b'") + "and 'stuff'")
    ("a'b'", "and 'stuff'")
    """
    return "{}{}{}".format(quote, value.replace(quote, quote + quote), quote)
