from __future__ import annotations

import abc

from dl_query_processing.compilation.primitives import CompiledQuery
from dl_query_processing.compilation.specs import QuerySpec


class RawQueryCompilerBase(abc.ABC):
    @abc.abstractmethod
    def make_compiled_query(self, query_spec: QuerySpec) -> CompiledQuery:
        raise NotImplementedError
