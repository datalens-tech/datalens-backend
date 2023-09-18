from __future__ import annotations

from contextlib import contextmanager
import http.client
import logging
import os
from typing import (
    Any,
    Callable,
    List,
    Literal,
    Tuple,
    overload,
)

import pytest

from dl_testing.containers import get_test_container_hostport
from dl_testing.shared_testing_constants import RUN_DEVHOST_TESTS
from dl_utils.wait import wait_for

LOGGER = logging.getLogger(__name__)


def skip_outside_devhost(func):
    """
    Not all tests can run in just any environment (particularly in autotests).
    So put those under an environment flag.
    Details:
    https://st.yandex-team.ru/DEVTOOLSSUPPORT-2022
    """
    if RUN_DEVHOST_TESTS:
        return func
    return pytest.mark.skip("Requires RUN_DEVHOST_TESTS=1")(func)


def wait_for_initdb(initdb_port, initdb_host=None, timeout=900, require: bool = False):
    initdb_host = initdb_host or get_test_container_hostport("init-db").host
    # TODO: initdb_port?

    def check_initdb_liveness() -> Tuple[bool, Any]:
        try:
            conn = http.client.HTTPConnection(initdb_host, initdb_port)
            conn.request("GET", "/")
            resp = conn.getresponse()
            body = resp.read().decode("utf-8", errors="replace")
            if resp.status != 200:
                raise Exception("Non-ok response", dict(res=resp, status=resp.status, body=body))
            return True, body
        except Exception as exc:
            return False, dict(exc=exc)

    return wait_for(
        name="initdb readiness",
        condition=check_initdb_liveness,
        timeout=timeout,
        require=require,
    )


@overload
def get_log_record(
    caplog: Any, predicate: Callable[[logging.LogRecord], bool], single: Literal[True] = True
) -> logging.LogRecord:
    pass


@overload
def get_log_record(
    caplog: Any, predicate: Callable[[logging.LogRecord], bool], single: Literal[False] = False
) -> List[logging.LogRecord]:
    pass


def get_log_record(caplog, predicate, single=False):
    log_records = [rec for rec in caplog.records if predicate(rec)]

    if single:
        assert len(log_records) == 1, f"Unexpected count of record in logs: {len(log_records)}"
        return log_records[0]

    return log_records


def guids_from_titles(result_schema: List[dict], titles: List[str]) -> List[str]:
    fields = [f["field"] if "field" in f else f for f in result_schema]
    guid_by_title = {f["title"]: f["guid"] for f in fields if f.get("title") in titles and f.get("guid")}
    return [guid_by_title[title] for title in titles]


@contextmanager
def override_env_cm(to_set: dict[str, str], purge: bool = False):
    preserved = {k: v for k, v in os.environ.items()}

    try:
        if purge:
            for k in os.environ:
                del os.environ[k]
        for k, v in to_set.items():
            os.environ[k] = v
        yield
    finally:
        for k in to_set:
            del os.environ[k]
            # os.unsetenv(k)

        for k, v in preserved.items():
            os.environ[k] = v
