import attr
from marshmallow import fields as ma_fields
from typing import Optional

from dl_constants.enums import (
    DashSQLQueryType,
    UserDataType,
)
from dl_model_tools.schema.base import (
    BaseSchema,
    DefaultSchema,
)
from dl_model_tools.schema.dynamic_enum_field import DynamicEnumField


@attr.s(frozen=True)
class TypedQueryRawParameters:
    path: str = attr.ib(kw_only=True)
    method: str = attr.ib(kw_only=True)
    body: Optional[dict] = attr.ib(kw_only=True)  # is really dict? or Any because we send it as is?


@attr.s(frozen=True)
class RawTypedQueryRaw:
    query_type: DashSQLQueryType = attr.ib(kw_only=True)
    parameters: TypedQueryRawParameters = attr.ib(kw_only=True)


class TypedQueryRawParametersSchema(DefaultSchema[TypedQueryRawParameters]):
    TARGET_CLS = TypedQueryRawParameters

    path = ma_fields.String(load_default="")
    method = ma_fields.String(required=True)
    body = ma_fields.Dict(load_default=None)  # or ma_fields.Raw?


class TypedQueryRawSchema(DefaultSchema[RawTypedQueryRaw]):
    TARGET_CLS = RawTypedQueryRaw

    query_type = DynamicEnumField(DashSQLQueryType, load_default=DashSQLQueryType.raw_query)
    parameters = ma_fields.Nested(TypedQueryRawParametersSchema, required=True)


# TODO:fix
class TypedQueryResultSchema(BaseSchema):
    class ColumnHeaderSchema(BaseSchema):
        name = ma_fields.String(required=True)
        data_type = ma_fields.Enum(UserDataType, required=True, attribute="user_type")

    # Raw result data will go here. MA is not good with serializing large amounts of data
    rows = ma_fields.Raw(required=True, attribute="data_rows")
    headers = ma_fields.List(ma_fields.Nested(ColumnHeaderSchema()), required=True, attribute="column_headers")
