""" ... """

from __future__ import annotations

import cProfile
from contextlib import contextmanager
import datetime
import logging
import os
from typing import (
    TYPE_CHECKING,
    Iterator,
    Optional,
)
import uuid

from dl_api_lib.enums import USPermissionKind
import dl_core.exc as common_exc


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


def need_permission_on_entry(us_entry: USEntry, permission: USPermissionKind) -> None:
    assert us_entry.permissions is not None
    assert us_entry.uuid is not None
    if not us_entry.permissions[permission.name]:
        raise common_exc.USPermissionRequired(us_entry.uuid, permission.name)
