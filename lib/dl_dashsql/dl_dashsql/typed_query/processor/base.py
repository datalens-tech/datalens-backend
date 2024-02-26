import abc

from dl_dashsql.typed_query.primitives import (
    TypedQuery,
    TypedQueryResult,
)


class TypedQueryProcessorBase(abc.ABC):
    @abc.abstractmethod
    async def process_typed_query(self, typed_query: TypedQuery) -> TypedQueryResult:
        raise NotImplementedError
