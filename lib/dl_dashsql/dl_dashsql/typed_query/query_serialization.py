import abc
from typing import (
    Generic,
    Type,
    TypeVar,
)

import attr
import marshmallow.fields as ma_fields
from marshmallow.schema import Schema

from dl_constants.enums import (
    DashSQLQueryType,
    UserDataType,
)
from dl_dashsql.typed_query.primitives import (
    PlainTypedQuery,
    TypedQueryBase,
    TypedQueryParameter,
    TypedQueryRaw,
    TypedQueryRawParameters,
)
from dl_model_tools.schema.base import DefaultSchema
from dl_model_tools.schema.dynamic_enum_field import DynamicEnumField
from dl_model_tools.schema.typed_values import (
    ValueSchema,
    WithNestedValueSchema,
)


_TYPED_QUERY_TV = TypeVar("_TYPED_QUERY_TV", bound=TypedQueryBase)


class TypedQuerySerializer(abc.ABC, Generic[_TYPED_QUERY_TV]):
    @abc.abstractmethod
    def serialize(self, typed_query: _TYPED_QUERY_TV) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def deserialize(self, typed_query_str: str) -> _TYPED_QUERY_TV:
        raise NotImplementedError


@attr.s
class MarshmallowTypedQuerySerializer(TypedQuerySerializer[_TYPED_QUERY_TV], Generic[_TYPED_QUERY_TV]):
    _schema_cls: Type[Schema] = attr.ib(kw_only=True)

    def serialize(self, typed_query: _TYPED_QUERY_TV) -> str:
        schema = self._schema_cls()
        typed_query_str = schema.dumps(typed_query)
        return typed_query_str

    def deserialize(self, typed_query_str: str) -> _TYPED_QUERY_TV:
        schema = self._schema_cls()
        typed_query = schema.loads(typed_query_str)
        return typed_query


class TypedQuerySerializerParameterSchema(DefaultSchema[TypedQueryParameter], WithNestedValueSchema):
    # TODO: Refactor the whole thing without ValueSchema?
    TARGET_CLS = TypedQueryParameter
    TYPE_FIELD_NAME = "user_type"

    name = ma_fields.String(required=True)
    user_type = ma_fields.Enum(UserDataType, required=True, attribute="typed_value.type", data_key="user_type")
    typed_value = ma_fields.Nested(ValueSchema, required=True)


class PlainTypedQuerySerializerSchema(DefaultSchema[PlainTypedQuery]):
    TARGET_CLS = PlainTypedQuery

    query_type = DynamicEnumField(DashSQLQueryType)
    query = ma_fields.String(required=True)
    parameters = ma_fields.List(ma_fields.Nested(TypedQuerySerializerParameterSchema), required=True)


class TypedQueryRawSerializerParametersSchema(DefaultSchema[TypedQueryRawParameters]):
    TARGET_CLS = TypedQueryRawParameters

    path = ma_fields.String()
    method = ma_fields.String(required=True)
    body = ma_fields.Raw()


class TypedQueryRawSerializerSchema(DefaultSchema[TypedQueryRaw]):
    TARGET_CLS = TypedQueryRaw

    parameters = ma_fields.Nested(TypedQueryRawSerializerParametersSchema, required=True)


_TYPED_QUERY_SERIALIZER_REGISTRY: dict[DashSQLQueryType, TypedQuerySerializer] = {
    DashSQLQueryType.generic_query: MarshmallowTypedQuerySerializer(schema_cls=PlainTypedQuerySerializerSchema),
    DashSQLQueryType.generic_label_values: MarshmallowTypedQuerySerializer(schema_cls=PlainTypedQuerySerializerSchema),
    DashSQLQueryType.generic_label_names: MarshmallowTypedQuerySerializer(schema_cls=PlainTypedQuerySerializerSchema),
    DashSQLQueryType.raw_query: MarshmallowTypedQuerySerializer(schema_cls=TypedQueryRawSerializerSchema),
}


def register_typed_query_serializer(query_type: DashSQLQueryType, serializer: TypedQuerySerializer) -> None:
    if existing_serializer := _TYPED_QUERY_SERIALIZER_REGISTRY.get(query_type) is not None:
        assert existing_serializer is serializer
    else:
        _TYPED_QUERY_SERIALIZER_REGISTRY[query_type] = serializer


def get_typed_query_serializer(query_type: DashSQLQueryType) -> TypedQuerySerializer:
    return _TYPED_QUERY_SERIALIZER_REGISTRY[query_type]
