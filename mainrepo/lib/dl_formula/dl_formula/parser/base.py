from __future__ import annotations

import abc
from collections import defaultdict
from contextlib import contextmanager
from functools import _CacheInfo
import os
import statistics
import time
from typing import (
    Dict,
    Generator,
    List,
    NamedTuple,
    Optional,
)

import attr

import dl_formula.core.exc as exc
import dl_formula.core.nodes as nodes
from dl_formula.inspect.function import (
    can_be_nonwindow,
    can_be_window,
    supports_bfb,
    supports_grouping,
    supports_ignore_dimensions,
    supports_lod,
    supports_ordering,
)

FORMULA_CACHE_SIZE = int(os.environ.get("FORMULA_CACHE_SIZE", 1000))


class ParserStats(NamedTuple):
    global_cache_hits: Dict[str, int]
    global_cache_misses: Dict[str, int]
    global_cache_maxsize: Dict[str, Optional[int]]
    global_cache_currsize: Dict[str, int]
    call_count: int
    call_time: float
    avg_length: Optional[float]
    length_counts: Dict[int, int]
    length_avg_times: Dict[int, float]
    length_median_times: Dict[int, float]


@attr.s
class FormulaParser(abc.ABC):
    call_count: int = attr.ib(init=False, default=0)
    call_time: float = attr.ib(init=False, default=0.0)
    length_sum: int = attr.ib(init=False, default=0)
    by_length: Dict[int, List[float]] = attr.ib(init=False, factory=lambda: defaultdict(list))

    @contextmanager
    def _parse_cm(self, formula: str) -> Generator[None, None, None]:
        started = time.thread_time()
        try:
            yield
        finally:
            call_time = time.thread_time() - started
            self.call_time += call_time
            self.call_count += 1
            self.by_length[len(formula)].append(call_time)

    def parse(self, formula: str) -> nodes.Formula:
        with self._parse_cm(formula=formula):
            return self._parse(formula)

    @abc.abstractmethod
    def _parse(self, formula: str) -> nodes.Formula:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_global_stats(self) -> Dict[str, _CacheInfo]:
        raise NotImplementedError

    def get_usage_stats(self) -> ParserStats:
        global_stats = self._get_global_stats()
        return ParserStats(
            global_cache_hits={key: stats.hits for key, stats in global_stats.items()},
            global_cache_misses={key: stats.misses for key, stats in global_stats.items()},
            global_cache_maxsize={key: stats.maxsize for key, stats in global_stats.items()},
            global_cache_currsize={key: stats.currsize for key, stats in global_stats.items()},
            call_count=self.call_count,
            call_time=self.call_time,
            avg_length=self.length_sum / self.call_count if self.call_count else None,
            length_counts={length: len(times) for length, times in self.by_length.items()},
            length_avg_times={length: statistics.mean(times) for length, times in self.by_length.items()},
            length_median_times={length: statistics.median(times) for length, times in self.by_length.items()},
        )


def parser_cache_qualifier(formula: str) -> str:
    """
    When used as a cache qualifier,
    function generates 3 separate caches for formula parser
    and routes calls to these caches by formula length.
    """
    length = len(formula)
    if length > 500:
        return "f500_inf"
    if length > 100:
        return "f100_500"
    return "f0_100"


class FunctionCapabilities(NamedTuple):
    is_window: bool


def resolve_function_capabilities(
    name: str,
    grouping: Optional[nodes.WindowGrouping] = None,
    ordering: Optional[nodes.Ordering] = None,
    before_filter_by: Optional[nodes.BeforeFilterBy] = None,
    ignore_dimensions: Optional[nodes.IgnoreDimensions] = None,
    lod: Optional[nodes.LodSpecifier] = None,
) -> FunctionCapabilities:
    can_be_window_res = can_be_window(name)
    can_be_nonwindow_res = can_be_nonwindow(name)

    is_window = False
    if can_be_window_res:
        if can_be_nonwindow_res:
            # For functions that can be both window and non-window
            # the type is determined by the presence of a grouping clause
            is_window = grouping is not None
        else:
            # Can only be window
            is_window = True

    func_t_str = "window" if is_window else "non-window"
    u_name = name.upper

    if grouping is not None and not supports_grouping(name, is_window=is_window):
        raise exc.ParseClauseError(f"Grouping clause is not supported by {func_t_str} function {u_name()}")
    if ordering is not None and not supports_ordering(name, is_window=is_window):
        raise exc.ParseClauseError(f"ORDER BY clause is not supported by {func_t_str} function {u_name()}")
    if lod is not None and not supports_lod(name, is_window=is_window):
        raise exc.ParseClauseError(f"Level-of-detail clause is not supported by {func_t_str} function {u_name()}")
    if ignore_dimensions is not None and not supports_ignore_dimensions(name, is_window=is_window):
        raise exc.ParseClauseError(f"IGNORE DIMENSIONS clause is not supported by {func_t_str} function {u_name()}")
    if before_filter_by is not None and not supports_bfb(name, is_window=is_window):
        raise exc.ParseClauseError(f"BEFORE FILTER BY clause is not supported by {func_t_str} function {u_name()}")

    return FunctionCapabilities(
        is_window=is_window,
    )
