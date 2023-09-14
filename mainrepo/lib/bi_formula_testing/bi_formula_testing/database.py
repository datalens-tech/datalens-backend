from __future__ import annotations

import datetime
from functools import partial
import logging
import time
from typing import (
    Optional,
    Union,
)

import attr
import pytz
import sqlalchemy as sa

from bi_db_testing.database.base import (
    DbBase,
    DbConfig,
)
from bi_db_testing.database.dispenser import DbDispenserBase
from bi_db_testing.database.engine_wrapper import get_engine_wrapper_cls_for_url
from bi_formula.core.dialect import DialectCombo
from bi_utils.wait import wait_for

LOGGER = logging.getLogger(__name__)


@attr.s(frozen=True)
class FormulaDbConfig(DbConfig):
    dialect: DialectCombo = attr.ib(kw_only=True)
    _tzinfo: Optional[datetime.tzinfo] = attr.ib(kw_only=True, default=None)

    @property
    def tzinfo(self) -> datetime.tzinfo:
        if self._tzinfo is not None:
            return self._tzinfo
        return pytz.UTC


@attr.s
class Db(DbBase[FormulaDbConfig]):
    @property
    def dialect(self) -> DialectCombo:
        return self.config.dialect

    @property
    def tzinfo(self) -> datetime.tzinfo:
        return self.config.tzinfo


def make_db_config(dialect: DialectCombo, url: str, tzinfo: Optional[datetime.tzinfo] = None) -> FormulaDbConfig:
    db_eng_config_cls = get_engine_wrapper_cls_for_url(url).CONFIG_CLS
    db_eng_config = db_eng_config_cls(url=url)
    db_config = FormulaDbConfig(
        engine_config=db_eng_config,
        dialect=dialect,
        tzinfo=tzinfo,
    )
    return db_config


def make_db_from_config(db_config: FormulaDbConfig) -> Db:
    ew_cls = get_engine_wrapper_cls_for_url(db_config.engine_config.url)
    engine_wrapper = ew_cls(config=db_config.engine_config)
    return Db(config=db_config, engine_wrapper=engine_wrapper)


@attr.s
class FormulaDbDispenser(DbDispenserBase[FormulaDbConfig, Db]):
    global_timeout: Union[int, float] = attr.ib(kw_only=True, default=180.0)
    poll_pause: Union[int, float] = attr.ib(kw_only=True, default=0.7)
    # internal
    db_is_up: dict[FormulaDbConfig, Db] = attr.ib(init=False, factory=dict)
    first_call_time: Optional[float] = attr.ib(init=False, default=None)

    def ensure_db_is_up(self, db: Db) -> tuple[bool, str]:
        check_value_in = 123
        try:
            check_value_res = db.scalar(sa.literal(check_value_in))
        except Exception as exc:
            return False, str(exc)
        else:
            assert check_value_res == check_value_in
            return True, ""

    def make_database(self, db_config: FormulaDbConfig) -> Db:
        # Share the 'max time to up' between all dialects:
        if self.first_call_time is None:
            self.first_call_time = time.monotonic()

        assert self.first_call_time is not None

        timelimit = self.first_call_time + self.global_timeout
        db = make_db_from_config(db_config)
        wait_condition = partial(self.ensure_db_is_up, db=db)
        logging.info(f"Waiting for db to come up: {db_config!r}")
        wait_for("Make database", condition=wait_condition, timeout=timelimit, interval=self.poll_pause)

        return db

    def get_database(self, db_config: FormulaDbConfig) -> Db:
        if db_config in self.db_is_up:
            return self.db_is_up[db_config]  # supposed to be alive already.

        db = self.make_database(db_config)
        self.db_is_up[db_config] = db
        return db
