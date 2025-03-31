from __future__ import annotations

import itertools
from typing import (
    Generator,
)

import attr


@attr.s
class IdGenerator:
    _used_ids: set[int] = attr.ib(factory=set)
    _item_counter: Generator[int, None, None] = attr.ib(init=False, factory=itertools.count)

    def add_id(self, item_id: int) -> None:
        self._used_ids.add(item_id)

    def generate_id(self) -> int:
        while True:
            item_id = next(self._item_counter)
            if item_id not in self._used_ids:
                self._used_ids.add(item_id)
                return item_id
