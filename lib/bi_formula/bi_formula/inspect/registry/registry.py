from functools import lru_cache
from typing import Dict

from bi_formula.inspect.registry.item import (
    BasicOpItem, BasicOpItemKey,
    NameIsWinOpItem, NameIsWinOpItemKey,
    NameOpItem,
)


@lru_cache(maxsize=10000)
def norm_name(func_name: str) -> str:
    return func_name.lower()


class LowlevelOpRegistry:
    def __init__(self):
        self._basic_op_registry: Dict[BasicOpItemKey, BasicOpItem] = {}
        self._name_is_win_op_registry: Dict[NameIsWinOpItemKey, NameIsWinOpItem] = {}
        self._name_op_registry: Dict[str, NameOpItem] = {}

    def add(self, basic_item: BasicOpItem) -> None:
        self._add_to_basic_op_registry(basic_item)
        self._add_to_name_is_win_op_registry(basic_item)
        self._add_to_name_op_registry(basic_item)

    def _add_to_basic_op_registry(self, basic_item: BasicOpItem) -> None:
        basic_key = BasicOpItemKey(
            name=basic_item.name,
            arg_cnt=basic_item.arg_cnt,
            is_window=basic_item.is_window,
            is_aggregate=basic_item.is_aggregate,
            is_function=basic_item.is_function,
        )
        ex_basic_item = self._basic_op_registry.get(basic_key)
        if ex_basic_item is not None:
            assert basic_item == ex_basic_item
        else:
            self._basic_op_registry[basic_key] = basic_item

    def _add_to_name_is_win_op_registry(self, basic_item: BasicOpItem) -> None:
        name_is_win_key = NameIsWinOpItemKey(
            name=basic_item.name,
            is_window=basic_item.is_window,
        )
        name_is_win_item = NameIsWinOpItem(
            name=basic_item.name,
            is_window=basic_item.is_window,
            uses_default_ordering=basic_item.uses_default_ordering,
            supports_grouping=basic_item.supports_grouping,
            supports_ordering=basic_item.supports_ordering,
            supports_lod=basic_item.supports_lod,
            supports_ignore_dimensions=basic_item.supports_ignore_dimensions,
            supports_bfb=basic_item.supports_bfb,
        )
        ex_name_is_win_item = self._name_is_win_op_registry.get(name_is_win_key)
        if ex_name_is_win_item is not None:
            assert name_is_win_item == ex_name_is_win_item
        else:
            self._name_is_win_op_registry[name_is_win_key] = name_is_win_item

    def _add_to_name_op_registry(self, basic_item: BasicOpItem) -> None:
        name_key = basic_item.name
        can_be_aggregate: bool = False
        can_be_window: bool = False
        can_be_nonwindow: bool = False
        ex_name_item = self._name_op_registry.get(name_key)
        if ex_name_item is not None:
            can_be_aggregate = ex_name_item.can_be_aggregate
            can_be_window = ex_name_item.can_be_window
            can_be_nonwindow = ex_name_item.can_be_nonwindow
        name_item = NameOpItem(
            name=basic_item.name,
            can_be_aggregate=basic_item.is_aggregate or can_be_aggregate,
            can_be_window=basic_item.is_window or can_be_window,
            can_be_nonwindow=(not basic_item.is_window) or can_be_nonwindow,
        )
        self._name_op_registry[name_key] = name_item

    def can_be_aggregate(self, name: str) -> bool:
        name = norm_name(name)
        item = self._name_op_registry.get(name)
        return item is not None and item.can_be_aggregate

    def can_be_window(self, name: str) -> bool:
        name = norm_name(name)
        item = self._name_op_registry.get(name)
        return item is not None and item.can_be_window

    def can_be_nonwindow(self, name: str) -> bool:
        name = norm_name(name)
        item = self._name_op_registry.get(name)
        return item is not None and item.can_be_nonwindow

    def supports_grouping(self, name: str, is_window: bool) -> bool:
        name = norm_name(name)
        key = NameIsWinOpItemKey(name=name, is_window=is_window)
        item = self._name_is_win_op_registry.get(key)
        return item is not None and item.supports_grouping

    def supports_ordering(self, name: str, is_window: bool) -> bool:
        name = norm_name(name)
        key = NameIsWinOpItemKey(name=name, is_window=is_window)
        item = self._name_is_win_op_registry.get(key)
        return item is not None and item.supports_ordering

    def supports_ignore_dimensions(self, name: str, is_window: bool) -> bool:
        name = norm_name(name)
        key = NameIsWinOpItemKey(name=name, is_window=is_window)
        item = self._name_is_win_op_registry.get(key)
        return item is not None and item.supports_ignore_dimensions

    def supports_lod(self, name: str, is_window: bool) -> bool:
        name = norm_name(name)
        key = NameIsWinOpItemKey(name=name, is_window=is_window)
        item = self._name_is_win_op_registry.get(key)
        return item is not None and item.supports_lod

    def supports_bfb(self, name: str, is_window: bool) -> bool:
        name = norm_name(name)
        key = NameIsWinOpItemKey(name=name, is_window=is_window)
        item = self._name_is_win_op_registry.get(key)
        return item is not None and item.supports_bfb

    def uses_default_ordering(self, name: str, is_window: bool) -> bool:
        name = norm_name(name)
        key = NameIsWinOpItemKey(name=name, is_window=is_window)
        item = self._name_is_win_op_registry.get(key)
        return item is not None and item.uses_default_ordering


LOWLEVEL_OP_REGISTRY = LowlevelOpRegistry()
