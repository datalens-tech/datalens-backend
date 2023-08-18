from marshmallow import fields as ma_fields

from bi_api_connector.api_schema.source_base import (
    DataSourceTemplateBaseSchema,
    SimpleParametersSchema, SimpleDataSourceSchema,
)

# Effectively `CHYTTableSourceSchema = SQLDataSourceSchema` because it is in `SQL_SOURCE_TYPES`.


class CHYTTableListParametersSchema(SimpleParametersSchema):
    table_names = ma_fields.String()  # newline-separated tables


class CHYTTableListDataSourceSchema(SimpleDataSourceSchema):
    parameters = ma_fields.Nested(CHYTTableListParametersSchema)


class CHYTTableListDataSourceTemplateSchema(DataSourceTemplateBaseSchema):
    parameters = ma_fields.Nested(CHYTTableListParametersSchema)


class CHYTTableRangeParametersSchema(SimpleParametersSchema):
    directory_path = ma_fields.String()
    range_from = ma_fields.String()
    range_to = ma_fields.String()


class CHYTTableRangeDataSourceSchema(SimpleDataSourceSchema):
    parameters = ma_fields.Nested(CHYTTableRangeParametersSchema)


class CHYTTableRangeDataSourceTemplateSchema(DataSourceTemplateBaseSchema):
    parameters = ma_fields.Nested(CHYTTableRangeParametersSchema)
