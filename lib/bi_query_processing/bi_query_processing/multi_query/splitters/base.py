from __future__ import annotations

import abc
from typing import Optional

from bi_query_processing.compilation.primitives import (
    CompiledQuery, CompiledMultiQueryBase,
)
from bi_query_processing.multi_query.tools import CompiledMultiQueryPatch
from bi_query_processing.utils.name_gen import PrefixedIdGen


class MultiQuerySplitterBase(abc.ABC):
    @abc.abstractmethod
    def split_query(
            self,
            query: CompiledQuery, requirement_subtree: CompiledMultiQueryBase,
            query_id_gen: PrefixedIdGen, expr_id_gen: PrefixedIdGen,
    ) -> Optional[CompiledMultiQueryPatch]:
        raise NotImplementedError
