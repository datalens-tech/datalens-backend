from __future__ import annotations

import logging

from marshmallow import fields as ma_fields

from dl_api_lib.enums import WhereClauseOperation
from dl_constants.enums import (
    AggregationFunction,
    BinaryJoinOperator,
    ConnectionType,
    DataSourceType,
    JoinType,
    UserDataType,
)
from dl_model_tools.schema.base import BaseSchema
from dl_model_tools.schema.dynamic_enum_field import DynamicEnumField


LOGGER = logging.getLogger(__name__)


class JoinSchema(BaseSchema):
    types = ma_fields.List(ma_fields.Enum(JoinType))
    operators = ma_fields.List(ma_fields.Enum(BinaryJoinOperator))


class PreviewSchema(BaseSchema):
    enabled = ma_fields.Boolean()


class DataTypesSchema(BaseSchema):
    class DataTypeListItemSchema(BaseSchema):
        type = ma_fields.Enum(UserDataType)
        aggregations = ma_fields.List(ma_fields.Enum(AggregationFunction))
        casts = ma_fields.List(ma_fields.Enum(UserDataType))
        filter_operations = ma_fields.List(ma_fields.Enum(WhereClauseOperation))

    items = ma_fields.List(ma_fields.Nested(DataTypeListItemSchema))


class FieldsSchema(BaseSchema):
    class FieldListItemSchema(BaseSchema):
        guid = ma_fields.String()
        casts = ma_fields.List(ma_fields.Enum(UserDataType))
        aggregations = ma_fields.List(ma_fields.Enum(AggregationFunction))

    items = ma_fields.List(ma_fields.Nested(FieldListItemSchema))


class CompatConnectionTypeListItemSchema(BaseSchema):
    conn_type = DynamicEnumField(ConnectionType)


class ConnectionsSchema(BaseSchema):
    class ConnectionListItemSchema(BaseSchema):
        id = ma_fields.String()
        replacement_types = ma_fields.List(ma_fields.Nested(CompatConnectionTypeListItemSchema))

    max = ma_fields.Integer()
    compatible_types = ma_fields.List(ma_fields.Nested(CompatConnectionTypeListItemSchema))
    items = ma_fields.List(ma_fields.Nested(ConnectionListItemSchema))


class CompatSourceTypeListItemSchema(BaseSchema):
    source_type = DynamicEnumField(DataSourceType)


class SourcesSchema(BaseSchema):
    class SourceListItemSchema(BaseSchema):
        id = ma_fields.String()
        schema_update_enabled = ma_fields.Boolean()

    max = ma_fields.Integer()
    compatible_types = ma_fields.List(ma_fields.Nested(CompatSourceTypeListItemSchema))
    items = ma_fields.List(ma_fields.Nested(SourceListItemSchema))


class AvatarsSchema(BaseSchema):
    class SourceListItemSchema(BaseSchema):
        id = ma_fields.String()
        schema_update_enabled = ma_fields.Boolean()

    max = ma_fields.Integer()
    items = ma_fields.List(ma_fields.Nested(SourceListItemSchema))


class OptionsSchema(BaseSchema):
    join = ma_fields.Nested(JoinSchema)
    schema_update_enabled = ma_fields.Boolean()
    data_types = ma_fields.Nested(DataTypesSchema)
    preview = ma_fields.Nested(PreviewSchema)
    fields_ = ma_fields.Nested(FieldsSchema, attribute="fields", data_key="fields")
    connections = ma_fields.Nested(ConnectionsSchema)
    sources = ma_fields.Nested(SourcesSchema)
    source_avatars = ma_fields.Nested(AvatarsSchema)
    supports_offset = ma_fields.Boolean()
    supported_functions = ma_fields.List(ma_fields.String())


class OptionsMixin(BaseSchema):
    options = ma_fields.Nested(OptionsSchema, dump_only=True)
