import abc
from typing import (
    Generic,
    TypeVar,
)

import attr
from marshmallow.schema import Schema

from dl_constants.enums import DashSQLQueryType
from dl_dashsql.typed_query.primitives import TypedQuery


_TYPED_QUERY_TV = TypeVar("_TYPED_QUERY_TV", bound=TypedQuery)


class TypedQuerySerializer(abc.ABC, Generic[_TYPED_QUERY_TV]):
    @abc.abstractmethod
    def serialize(self, typed_query: _TYPED_QUERY_TV) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def deserialize(self, typed_query_str: str) -> _TYPED_QUERY_TV:
        raise NotImplementedError


@attr.s
class MarshmallowTypedQuerySerializer(TypedQuerySerializer[_TYPED_QUERY_TV], Generic[_TYPED_QUERY_TV]):
    _schema: Schema = attr.ib(kw_only=True)

    def serialize(self, typed_query: _TYPED_QUERY_TV) -> str:
        typed_query_str = self._schema.dumps(typed_query)
        return typed_query_str

    def deserialize(self, typed_query_str: str) -> _TYPED_QUERY_TV:
        typed_query = self._schema.loads(typed_query_str)
        return typed_query


_TYPED_QUERY_SERIALIZER_REGISTRY: dict[DashSQLQueryType, TypedQuerySerializer] = {}


def register_typed_query_serializer(query_type: DashSQLQueryType, serializer: TypedQuerySerializer) -> None:
    if existing_serializer := _TYPED_QUERY_SERIALIZER_REGISTRY.get(query_type) is not None:
        assert existing_serializer is serializer
    else:
        _TYPED_QUERY_SERIALIZER_REGISTRY[query_type] = serializer


def get_typed_query_serializer(query_type: DashSQLQueryType) -> TypedQuerySerializer:
    return _TYPED_QUERY_SERIALIZER_REGISTRY[query_type]
