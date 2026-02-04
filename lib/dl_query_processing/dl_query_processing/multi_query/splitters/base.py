from __future__ import annotations

import abc

from dl_query_processing.compilation.primitives import (
    CompiledMultiQueryBase,
    CompiledQuery,
)
from dl_query_processing.multi_query.tools import CompiledMultiQueryPatch
from dl_query_processing.utils.name_gen import PrefixedIdGen


class MultiQuerySplitterBase(abc.ABC):
    @abc.abstractmethod
    def split_query(
        self,
        query: CompiledQuery,
        requirement_subtree: CompiledMultiQueryBase,
        query_id_gen: PrefixedIdGen,
        expr_id_gen: PrefixedIdGen,
    ) -> CompiledMultiQueryPatch | None:
        raise NotImplementedError
