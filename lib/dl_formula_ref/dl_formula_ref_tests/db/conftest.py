from __future__ import annotations

import time
from typing import MutableSet

import pytest

from dl_formula.core.dialect import DialectCombo
from dl_formula.definitions.literals import literal
from dl_formula_testing.database import (
    Db,
    make_db_config,
    make_db_from_config,
)
from dl_formula_testing.evaluator import DbEvaluator
from dl_testing.containers import get_test_container_hostport

from dl_connector_clickhouse.formula.constants import ClickHouseDialect


ALL_DB_CONFIGURATIONS = {
    ClickHouseDialect.CLICKHOUSE_22_10: (
        f'clickhouse://datalens:qwerty@{get_test_container_hostport("db-clickhouse-22-10", fallback_port=51110).as_pair()}/datalens_test'
    ),
}
EXCLUDE_DIALECTS = []
DB_CONFIGURATIONS = {
    dialect: url for dialect, url in ALL_DB_CONFIGURATIONS.items() if url and dialect not in EXCLUDE_DIALECTS
}


@pytest.fixture(scope="session")
def all_db_configurations():
    return ALL_DB_CONFIGURATIONS


def base_db_for(dialect: DialectCombo) -> Db:
    url = ALL_DB_CONFIGURATIONS[dialect]
    assert url
    db = make_db_from_config(make_db_config(url=url, dialect=dialect))
    return db


def alive_db_for(dialect, timelimit, poll_pause):
    check_value_in = 123
    while True:
        db = base_db_for(dialect)
        try:
            check_value_res = db.scalar(literal(check_value_in, d=dialect))
        except Exception as exc:
            if time.monotonic() > timelimit:
                raise Exception(f"Timed out while waiting for db {dialect!r} {db!r}", exc)
            print(f"Waiting for db to come up: {dialect!r}   (db={db!r}, exc={exc!r}).")
            time.sleep(poll_pause)
        else:
            assert check_value_res == check_value_in
            break
    return db


class DbStateTracking:
    """State-keeper for db liveness, to be used as a singleton"""

    def __init__(self):
        self.db_is_up: MutableSet[DialectCombo] = set()
        self.first_call_time = None

    def get_db(self, dialect, global_timeout, poll_pause):
        if dialect in self.db_is_up:
            return base_db_for(dialect)  # supposed to be alive already.

        # Share the 'max time to up' between all dialects:
        if self.first_call_time is None:
            self.first_call_time = time.monotonic()

        timelimit = self.first_call_time + global_timeout
        db = alive_db_for(dialect=dialect, timelimit=timelimit, poll_pause=poll_pause)
        self.db_is_up.add(dialect)
        return db


DB_STATE_TRACKING = DbStateTracking()


def dbe_for(dialect: DialectCombo, wait_for_up_timeout=180.0, wait_for_up_pause=0.7) -> DbEvaluator:
    db = DB_STATE_TRACKING.get_db(dialect=dialect, global_timeout=wait_for_up_timeout, poll_pause=wait_for_up_pause)
    dbe = DbEvaluator(db=db)
    return dbe


@pytest.fixture(
    scope="session",
    params=[dialect for dialect in sorted(DB_CONFIGURATIONS)],
    ids=[d.common_name_and_version for d in sorted(DB_CONFIGURATIONS)],
)
def any_dialect(request) -> DialectCombo:
    return request.param


@pytest.fixture(scope="session")
def dbe(any_dialect):
    return dbe_for(any_dialect)
