from typing import Any

from marshmallow import fields as ma_fields, EXCLUDE
from marshmallow_oneofschema import OneOfSchema
from marshmallow_enum import EnumField

from bi_constants.enums import (
    BIType, FieldRole, FieldType, FieldVisibility, LegendItemType, OrderDirection, RangeType,
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
    type_field = 'role'

    class RoleSpecSchemaVariant(BaseSchema):
        role = EnumField(FieldRole)

    class OrderByRoleSpecSchemaVariant(RoleSpecSchemaVariant):
        direction = EnumField(OrderDirection)

    class AnnotationRoleSpecSchemaVariant(RoleSpecSchemaVariant):
        annotation_type = ma_fields.String()
        target_legend_item_ids = ma_fields.List(ma_fields.Integer(), allow_none=True)

    class RangeRoleSpecSchemaVariant(RoleSpecSchemaVariant):
        range_type = EnumField(RangeType)

    class DimensionRoleSpecSchemaVariant(RoleSpecSchemaVariant):
        visibility = EnumField(FieldVisibility)

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
    data_type = EnumField(BIType)
    field_type = EnumField(FieldType)
    item_type = EnumField(LegendItemType)


class LegendSchema(BaseSchema):
    fields_ = ma_fields.Nested(LegendItemSchema, data_key='fields', many=True, attribute='items')
