import abc
from typing import ClassVar

import attr
import marshmallow.fields as ma_fields
from marshmallow.schema import Schema

from dl_constants.enums import (
    DashSQLQueryType,
    UserDataType,
)
from dl_dashsql.typed_query.primitives import (
    TypedQueryRawResult,
    TypedQueryResult,
)
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


class TypedQueryRawResultSerializerSchema(DefaultSchema[TypedQueryRawResult]):
    TARGET_CLS = TypedQueryRawResult

    class ResultDataSchema(BaseSchema):
        status = ma_fields.Integer(required=True)
        headers = ma_fields.Dict()
        body = ma_fields.Dict()

    query_type = DynamicEnumField(DashSQLQueryType)
    data = ma_fields.Nested(ResultDataSchema(), required=True)


@attr.s
class DefaultTypedQueryResultSerializer(TypedQueryResultSerializer):
    _schema: ClassVar[Schema] = TypedQueryResultSerializerSchema()

    def serialize(self, typed_query_result: TypedQueryResult) -> str:
        typed_query_result_str = self._schema.dumps(typed_query_result)
        return typed_query_result_str

    def deserialize(self, typed_query_result_str: str) -> TypedQueryResult:
        typed_query_result = self._schema.loads(typed_query_result_str)
        return typed_query_result


@attr.s
class DefaultTypedQueryRawResultSerializer:
    _schema: ClassVar[Schema] = TypedQueryRawResultSerializerSchema()

    def serialize(self, typed_query_raw_result: TypedQueryRawResult) -> str:
        typed_query_raw_result_str = self._schema.dumps(typed_query_raw_result)
        return typed_query_raw_result_str

    def deserialize(self, typed_query_raw_result_str: str) -> TypedQueryRawResult:
        typed_query_raw_result = self._schema.loads(typed_query_raw_result_str)
        return typed_query_raw_result


def get_typed_query_result_serializer(query_type: DashSQLQueryType) -> TypedQueryResultSerializer:
    return DefaultTypedQueryResultSerializer()
