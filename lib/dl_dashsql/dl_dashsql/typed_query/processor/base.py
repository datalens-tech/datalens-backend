import abc

from dl_dashsql.typed_query.primitives import (
    TypedQuery,
    TypedQueryRaw,
    TypedQueryRawResult,
    TypedQueryResult,
)


class TypedQueryProcessorBase(abc.ABC):
    @abc.abstractmethod
    async def process_typed_query(self, typed_query: TypedQuery) -> TypedQueryResult:
        raise NotImplementedError


class TypedQueryRawProcessorBase(abc.ABC):
    @abc.abstractmethod
    async def process_typed_query_raw(self, typed_query_raw: TypedQueryRaw) -> TypedQueryRawResult:
        raise NotImplementedError
