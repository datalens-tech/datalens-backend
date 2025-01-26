from typing import Optional

import attr
from marshmallow import fields as ma_fields

from dl_constants.enums import DashSQLQueryType
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
    body = ma_fields.Dict(load_default=dict)


class TypedQueryRawSchema(DefaultSchema[RawTypedQueryRaw]):
    TARGET_CLS = RawTypedQueryRaw

    query_type = DynamicEnumField(DashSQLQueryType, load_default=DashSQLQueryType.raw_query)
    parameters = ma_fields.Nested(TypedQueryRawParametersSchema, required=True)


class TypedQueryRawResultDataSchema(BaseSchema):
    body = ma_fields.Dict(load_default=dict)
    headers = ma_fields.Dict()
    status = ma_fields.Integer(required=True)


class TypedQueryRawResultSchema(BaseSchema):
    data = ma_fields.Nested(TypedQueryRawResultDataSchema)
