import abc
from typing import ClassVar

import attr
import marshmallow.fields as ma_fields
from marshmallow.schema import Schema

from dl_constants.enums import (
    DashSQLQueryType,
    UserDataType,
)
from dl_dashsql.typed_query.primitives import TypedQueryResult
from dl_model_tools.schema.base import (
    BaseSchema,
    DefaultSchema,
)
from dl_model_tools.schema.dynamic_enum_field import DynamicEnumField


class TypedQueryResultSerializer(abc.ABC):
    @abc.abstractmethod
    def serialize(self, typed_query_result: TypedQueryResult) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def deserialize(self, typed_query_result_str: str) -> TypedQueryResult:
        raise NotImplementedError


class TypedQueryResultSerializerSchema(DefaultSchema[TypedQueryResult]):
    TARGET_CLS = TypedQueryResult

    class ColumnHeaderSchema(BaseSchema):
        name = ma_fields.String(required=True)
        data_type = ma_fields.Enum(UserDataType, required=True, attribute="user_type")

    query_type = DynamicEnumField(DashSQLQueryType)
    rows = ma_fields.Raw(required=True, attribute="data_rows")
    headers = ma_fields.List(ma_fields.Nested(ColumnHeaderSchema()), required=True, attribute="column_headers")


@attr.s
class DefaultTypedQueryResultSerializer(TypedQueryResultSerializer):
    _schema: ClassVar[Schema] = TypedQueryResultSerializerSchema()

    def serialize(self, typed_query_result: TypedQueryResult) -> str:
        typed_query_result_str = self._schema.dumps(typed_query_result)
        return typed_query_result_str

    def deserialize(self, typed_query_result_str: str) -> TypedQueryResult:
        typed_query_result = self._schema.loads(typed_query_result_str)
        return typed_query_result


def get_typed_query_result_serializer(query_type: DashSQLQueryType) -> TypedQueryResultSerializer:
    return DefaultTypedQueryResultSerializer()
