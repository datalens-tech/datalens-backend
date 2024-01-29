import abc
from typing import (
    Generic,
    TypeVar,
)

import attr
from marshmallow.schema import Schema

from dl_constants.enums import DashSQLQueryType
from dl_dashsql.typed_query.primitives import TypedQueryResult


_TYPED_QUERY_RESULT_TV = TypeVar("_TYPED_QUERY_RESULT_TV", bound=TypedQueryResult)


class TypedQueryResultSerializer(abc.ABC, Generic[_TYPED_QUERY_RESULT_TV]):
    @abc.abstractmethod
    def serialize(self, typed_query_result: _TYPED_QUERY_RESULT_TV) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def deserialize(self, typed_query_result_str: str) -> _TYPED_QUERY_RESULT_TV:
        raise NotImplementedError


@attr.s
class MarshmallowTypedQueryResultSerializer(
    TypedQueryResultSerializer[_TYPED_QUERY_RESULT_TV],
    Generic[_TYPED_QUERY_RESULT_TV],
):
    _schema: Schema = attr.ib(kw_only=True)

    def serialize(self, typed_query_result: _TYPED_QUERY_RESULT_TV) -> str:
        typed_query_result_str = self._schema.dumps(typed_query_result)
        return typed_query_result_str

    def deserialize(self, typed_query_result_str: str) -> _TYPED_QUERY_RESULT_TV:
        typed_query_result = self._schema.loads(typed_query_result_str)
        return typed_query_result


_TYPED_QUERY_RESULT_SERIALIZER_REGISTRY: dict[DashSQLQueryType, TypedQueryResultSerializer] = {}


def register_typed_query_result_serializer(
    query_type: DashSQLQueryType,
    serializer: TypedQueryResultSerializer,
) -> None:
    if existing_serializer := _TYPED_QUERY_RESULT_SERIALIZER_REGISTRY.get(query_type) is not None:
        assert existing_serializer is serializer
    else:
        _TYPED_QUERY_RESULT_SERIALIZER_REGISTRY[query_type] = serializer


def get_typed_query_result_serializer(query_type: DashSQLQueryType) -> TypedQueryResultSerializer:
    return _TYPED_QUERY_RESULT_SERIALIZER_REGISTRY[query_type]
