from __future__ import annotations

import abc
from collections import defaultdict
from typing import (
    Callable,
    Dict,
    Generic,
    Optional,
    TypeVar,
)

import attr

from dl_db_testing.database.base import (
    DbBase,
    DbConfig,
)
from dl_utils.wait import wait_for


_DB_CONFIG_TV = TypeVar("_DB_CONFIG_TV", bound=DbConfig)
_DB_TV = TypeVar("_DB_TV", bound=DbBase)


@attr.s
class DbDispenserBase(abc.ABC, Generic[_DB_CONFIG_TV, _DB_TV]):
    _wait_on_init: bool = attr.ib(kw_only=True, default=True)
    _default_reconnect_timeout: int = attr.ib(kw_only=True, default=600)

    @abc.abstractmethod
    def make_database(self, db_config: _DB_CONFIG_TV) -> _DB_TV:
        raise NotImplementedError

    def wait_for_db(self, db: _DB_TV, reconnect_timeout: Optional[int] = None) -> None:
        if reconnect_timeout is None:
            reconnect_timeout = self._default_reconnect_timeout
        wait_for(name=f"test_db_{db.config}", condition=db.test, timeout=reconnect_timeout)

    def initialize_db(self, db: _DB_TV) -> None:
        if self._wait_on_init:
            if not db.test():
                self.wait_for_db(db=db)

    def get_database(self, db_config: _DB_CONFIG_TV) -> _DB_TV:
        db = self.make_database(db_config)
        self.initialize_db(db)
        return db


@attr.s
class ReInitableDbDispenser(DbDispenserBase[_DB_CONFIG_TV, _DB_TV], Generic[_DB_CONFIG_TV, _DB_TV]):
    _max_reinit_count: int = attr.ib(kw_only=True, default=4)
    _db_cache: Dict[_DB_CONFIG_TV, _DB_TV] = attr.ib(init=False, factory=dict)
    _reinit_hooks: Dict[_DB_CONFIG_TV, Callable[[], None]] = attr.ib(init=False, factory=dict)
    _db_reinit_counts: Dict[_DB_CONFIG_TV, int] = attr.ib(init=False, factory=lambda: defaultdict(lambda: 0))

    def get_database(self, db_config: _DB_CONFIG_TV) -> _DB_TV:
        if db_config not in self._db_cache:
            self._db_cache[db_config] = super().get_database(db_config)
        self._check_reinit_db(db_config=db_config)
        return self._db_cache[db_config]

    def add_reinit_hook(self, *, db_config: _DB_CONFIG_TV, reinit_hook: Callable[[], None]) -> None:
        self._reinit_hooks[db_config] = reinit_hook

    def _check_reinit_db(self, db_config: DbConfig, reconnect_timeout: Optional[int] = None) -> bool:
        """Check if DB is alive. If it is not, try to re-initialize it"""
        db = self._db_cache.get(db_config)  # type: ignore  # 2024-01-29 # TODO: Argument 1 to "get" of "dict" has incompatible type "DbConfig"; expected "_DB_CONFIG_TV"  [arg-type]
        if db is None:
            return False

        reinit_hook = self._reinit_hooks.get(db_config)  # type: ignore  # 2024-01-29 # TODO: Argument 1 to "get" of "dict" has incompatible type "DbConfig"; expected "_DB_CONFIG_TV"  [arg-type]
        if reinit_hook is None or self._db_reinit_counts[db_config] >= self._max_reinit_count:  # type: ignore  # 2024-01-29 # TODO: Invalid index type "DbConfig" for "dict[_DB_CONFIG_TV, int]"; expected type "_DB_CONFIG_TV"  [index]
            return False

        if not db.test():
            # DB is unavailable, so re-initialize the DB is possible
            reinit_hook()
            # Wait until it comes up
            self.wait_for_db(db=db, reconnect_timeout=reconnect_timeout)
            self._db_reinit_counts[db_config] += 1  # type: ignore  # 2024-01-29 # TODO: Invalid index type "DbConfig" for "dict[_DB_CONFIG_TV, int]"; expected type "_DB_CONFIG_TV"  [index]
            return True

        return False

    def check_reinit_all(self, reconnect_timeout: Optional[int] = None) -> None:
        for db_config, _db in self._db_cache.items():
            self._check_reinit_db(db_config=db_config, reconnect_timeout=reconnect_timeout)
