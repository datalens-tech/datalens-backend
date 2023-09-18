from typing import Callable

import attr
from pytest import MonkeyPatch

from dl_query_processing.compilation.primitives import CompiledMultiQueryBase
from dl_query_processing.translation.multi_level_translator import MultiLevelQueryTranslator


@attr.s
class MultiQueryInterceptor:
    _mpatch: MonkeyPatch = attr.ib(kw_only=True)
    _callback: Callable[[CompiledMultiQueryBase], None] = attr.ib(kw_only=True)
    _intercepted: bool = attr.ib(init=False, default=False)

    @property
    def intercepted(self) -> bool:
        return self._intercepted

    def __attrs_post_init__(self) -> None:
        self._prepare()

    def _prepare(self) -> None:
        self._mpatch.setattr(MultiLevelQueryTranslator, '_log_query_complexity_stats', self._log_query_complexity_stats)

    def _log_query_complexity_stats(
            self, compiled_multi_query: CompiledMultiQueryBase
    ) -> None:
        self._callback(compiled_multi_query)
        self._intercepted = True


def count_joins(multi_query: CompiledMultiQueryBase) -> int:
    result = 0
    for query in multi_query.iter_queries():
        result += len(query.join_on)
    return result
