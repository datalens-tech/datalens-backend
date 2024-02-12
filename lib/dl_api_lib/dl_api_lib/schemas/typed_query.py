import attr
from marshmallow import fields as ma_fields

from dl_api_lib.schemas.values import (
    ValueSchema,
    WithNestedValueSchema,
)
from dl_constants.enums import (
    DashSQLQueryType,
    UserDataType,
)
from dl_core.values import BIValue
from dl_model_tools.schema.base import (
    BaseSchema,
    DefaultSchema,
)
from dl_model_tools.schema.dynamic_enum_field import DynamicEnumField


@attr.s(frozen=True)
class RawTypedQueryParameter:
    name: str = attr.ib(kw_only=True)
    data_type: UserDataType = attr.ib(kw_only=True)
    value: BIValue = attr.ib(kw_only=True)


@attr.s(frozen=True)
class RawTypedQuery:
    query_type: DashSQLQueryType = attr.ib(kw_only=True)
    query_content: dict = attr.ib(kw_only=True)
    parameters: list[RawTypedQueryParameter] = attr.ib(kw_only=True)


class TypedQueryParameterSchema(DefaultSchema[RawTypedQueryParameter], WithNestedValueSchema):
    TARGET_CLS = RawTypedQueryParameter
    TYPE_FIELD_NAME = "data_type"

    name = ma_fields.String(required=True)
    data_type = ma_fields.Enum(UserDataType, required=True)
    value = ma_fields.Nested(ValueSchema, required=True)


class PlainTypedQueryContentSchema(BaseSchema):
    query = ma_fields.String(required=True)


class TypedQuerySchema(DefaultSchema[RawTypedQuery]):
    TARGET_CLS = RawTypedQuery

    query_type = DynamicEnumField(DashSQLQueryType, required=True)
    query_content = ma_fields.Raw(required=True)
    parameters = ma_fields.List(ma_fields.Nested(TypedQueryParameterSchema), load_default=None)


class DataRowsTypedQueryResultSchema(BaseSchema):
    class ColumnHeaderSchema(BaseSchema):
        name = ma_fields.String(required=True)
        data_type = ma_fields.Enum(UserDataType, required=True, attribute="user_type")

    query_type = DynamicEnumField(DashSQLQueryType, required=True)
    # Raw result data will go here. MA is not good with serializing large amounts of data
    rows = ma_fields.Raw(required=True, attribute="data_rows")
    headers = ma_fields.List(ma_fields.Nested(ColumnHeaderSchema()), required=True, attribute="column_headers")
