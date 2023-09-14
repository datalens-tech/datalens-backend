from typing import Any

from marshmallow import EXCLUDE
from marshmallow import fields as ma_fields
from marshmallow_oneofschema import OneOfSchema

from bi_constants.enums import (
    BIType,
    FieldRole,
    FieldType,
    FieldVisibility,
    LegendItemType,
    OrderDirection,
    RangeType,
)
from bi_model_tools.schema.base import BaseSchema
from bi_query_processing.legend.field_legend import RoleSpec


class DimensionValueSpecSchema(BaseSchema):
    legend_item_id = ma_fields.Integer()
    value = ma_fields.Raw()


class RoleSpecSchema(OneOfSchema):
    class Meta:
        unknown = EXCLUDE

    type_field_remove = False
    type_field = "role"

    class RoleSpecSchemaVariant(BaseSchema):
        role = ma_fields.Enum(FieldRole)

    class OrderByRoleSpecSchemaVariant(RoleSpecSchemaVariant):
        direction = ma_fields.Enum(OrderDirection)

    class AnnotationRoleSpecSchemaVariant(RoleSpecSchemaVariant):
        annotation_type = ma_fields.String()
        target_legend_item_ids = ma_fields.List(ma_fields.Integer(), allow_none=True)

    class RangeRoleSpecSchemaVariant(RoleSpecSchemaVariant):
        range_type = ma_fields.Enum(RangeType)

    class DimensionRoleSpecSchemaVariant(RoleSpecSchemaVariant):
        visibility = ma_fields.Enum(FieldVisibility)

    class RowRoleSpecSchemaVariant(DimensionRoleSpecSchemaVariant):
        pass

    class TemplateRoleSpecSchemaVariant(DimensionRoleSpecSchemaVariant):
        template = ma_fields.String()

    class TreeRoleSpecSchemaVariant(DimensionRoleSpecSchemaVariant):
        level = ma_fields.Integer()
        prefix = ma_fields.Raw()
        dimension_values = ma_fields.Nested(DimensionValueSpecSchema, many=True, allow_none=True)

    type_schemas = {
        FieldRole.row.name: RowRoleSpecSchemaVariant,
        FieldRole.measure.name: RoleSpecSchemaVariant,
        FieldRole.info.name: RoleSpecSchemaVariant,
        FieldRole.order_by.name: OrderByRoleSpecSchemaVariant,
        FieldRole.filter.name: RoleSpecSchemaVariant,  # TODO: extend if needed
        FieldRole.parameter.name: RoleSpecSchemaVariant,  # TODO: extend if needed
        FieldRole.range.name: RangeRoleSpecSchemaVariant,
        FieldRole.distinct.name: RoleSpecSchemaVariant,
        FieldRole.template.name: TemplateRoleSpecSchemaVariant,
        FieldRole.total.name: RoleSpecSchemaVariant,
        FieldRole.tree.name: TreeRoleSpecSchemaVariant,
    }

    def get_obj_type(self, obj: Any) -> str:
        assert isinstance(obj, RoleSpec)
        return obj.role.name


class LegendItemSchema(BaseSchema):
    legend_item_id = ma_fields.Integer()
    id = ma_fields.String()
    title = ma_fields.String()
    role_spec = ma_fields.Nested(RoleSpecSchema)
    data_type = ma_fields.Enum(BIType)
    field_type = ma_fields.Enum(FieldType)
    item_type = ma_fields.Enum(LegendItemType)


class LegendSchema(BaseSchema):
    fields_ = ma_fields.Nested(LegendItemSchema, data_key="fields", many=True, attribute="items")
