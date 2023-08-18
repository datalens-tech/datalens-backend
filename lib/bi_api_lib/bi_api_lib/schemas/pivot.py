from typing import Any

from marshmallow import fields as ma_fields, EXCLUDE, validate as ma_validate
from marshmallow_oneofschema import OneOfSchema
from marshmallow_enum import EnumField

from bi_constants.enums import PivotHeaderRole, PivotRole, OrderDirection

from bi_model_tools.schema.base import BaseSchema, DefaultSchema

from bi_api_lib.pivot.primitives import (
    PivotHeaderRoleSpec, PivotHeaderValue, PivotMeasureSortingSettings, PivotMeasureSorting, PivotHeaderInfo
)
from bi_api_lib.query.formalization.raw_pivot_specs import (
    RawPivotRoleSpec, RawAnnotationPivotRoleSpec, RawDimensionPivotRoleSpec, RawPivotMeasureRoleSpec,
    RawPivotLegendItem, RawPivotSpec, RawPivotTotalsItemSpec, RawPivotTotalsSpec,
    PivotPaginationSpec,
)
from bi_api_lib.query.formalization.pivot_legend import PivotRoleSpec


class PivotHeaderRoleSpecSchema(DefaultSchema[PivotHeaderRoleSpec]):
    TARGET_CLS = PivotHeaderRoleSpec

    role = EnumField(PivotHeaderRole, required=True)


class PivotHeaderValueSchema(DefaultSchema[PivotHeaderValue]):
    TARGET_CLS = PivotHeaderValue

    value = ma_fields.String(allow_none=False, required=True)


class PivotMeasureSortingSettingsSchema(DefaultSchema[PivotMeasureSortingSettings]):
    TARGET_CLS = PivotMeasureSortingSettings

    header_values = ma_fields.List(ma_fields.Nested(PivotHeaderValueSchema), allow_none=False, required=True)
    direction = EnumField(OrderDirection, load_default=OrderDirection.asc)
    role_spec = ma_fields.Nested(PivotHeaderRoleSpecSchema, allow_none=False, required=True)


class PivotMeasureSortingSchema(DefaultSchema[PivotMeasureSorting]):
    TARGET_CLS = PivotMeasureSorting

    column = ma_fields.Nested(PivotMeasureSortingSettingsSchema, allow_none=True, load_default=None)
    row = ma_fields.Nested(PivotMeasureSortingSettingsSchema, allow_none=True, load_default=None)


class PivotRoleSpecSchema(OneOfSchema):
    class Meta:
        unknown = EXCLUDE

    type_field_remove = False
    type_field = 'role'

    class PivotRoleSpecSchemaVariant(DefaultSchema[RawPivotRoleSpec]):
        TARGET_CLS = RawPivotRoleSpec

        role = EnumField(PivotRole, required=True)

    class DimensionPivotRoleSpecSchemaVariant(DefaultSchema[RawDimensionPivotRoleSpec]):
        TARGET_CLS = RawDimensionPivotRoleSpec

        role = EnumField(PivotRole, required=True)
        direction = EnumField(OrderDirection, load_default=OrderDirection.asc)

    class AnnotationPivotRoleSpecSchemaVariant(DefaultSchema[RawAnnotationPivotRoleSpec]):
        TARGET_CLS = RawAnnotationPivotRoleSpec

        role = EnumField(PivotRole, required=True)
        annotation_type = ma_fields.String(required=True)
        target_legend_item_ids = ma_fields.List(ma_fields.Integer(), allow_none=True)

    class MeasurePivotRoleSpecSchemaVariant(DefaultSchema[RawPivotMeasureRoleSpec]):
        TARGET_CLS = RawPivotMeasureRoleSpec

        role = EnumField(PivotRole, required=True)
        sorting = ma_fields.Nested(PivotMeasureSortingSchema, allow_none=True, load_default=None)

    type_schemas = {
        PivotRole.pivot_row.name: DimensionPivotRoleSpecSchemaVariant,
        PivotRole.pivot_column.name: DimensionPivotRoleSpecSchemaVariant,
        PivotRole.pivot_measure.name: MeasurePivotRoleSpecSchemaVariant,
        PivotRole.pivot_annotation.name: AnnotationPivotRoleSpecSchemaVariant,
        PivotRole.pivot_info.name: PivotRoleSpecSchemaVariant,
    }

    def get_obj_type(self, obj: Any) -> str:
        assert isinstance(obj, PivotRoleSpec)
        return obj.role.name


class RequestPivotItemSchema(DefaultSchema[RawPivotLegendItem]):
    TARGET_CLS = RawPivotLegendItem

    pivot_item_id = ma_fields.Integer(allow_none=True, required=False)
    legend_item_ids = ma_fields.List(ma_fields.Integer(allow_none=False), allow_none=False, required=True)
    role_spec = ma_fields.Nested(PivotRoleSpecSchema, allow_none=False, required=True)
    title = ma_fields.String(allow_none=True, required=False)


class PivotItemSchema(BaseSchema):
    pivot_item_id = ma_fields.Integer(allow_none=True)
    legend_item_ids = ma_fields.List(ma_fields.Integer(allow_none=False), allow_none=False, required=True)
    role_spec = ma_fields.Nested(PivotRoleSpecSchema, allow_none=False, required=True)
    title = ma_fields.String()


class PivotPaginationSchema(DefaultSchema[PivotPaginationSpec]):
    TARGET_CLS = PivotPaginationSpec

    limit_rows = ma_fields.Integer(load_default=None, validate=ma_validate.Range(min=1))
    offset_rows = ma_fields.Integer(load_default=None, validate=ma_validate.Range(min=0))


class PivotTotalsItemSchema(DefaultSchema[RawPivotTotalsItemSpec]):
    TARGET_CLS = RawPivotTotalsItemSpec

    level = ma_fields.Integer(allow_none=True, required=True)


class PivotTotalsSchema(DefaultSchema[RawPivotTotalsSpec]):
    TARGET_CLS = RawPivotTotalsSpec

    rows = ma_fields.Nested(PivotTotalsItemSchema, many=True, required=True)
    columns = ma_fields.Nested(PivotTotalsItemSchema, many=True, required=True)


class RequestPivotSpecSchema(DefaultSchema[RawPivotSpec]):
    TARGET_CLS = RawPivotSpec

    structure = ma_fields.Nested(RequestPivotItemSchema, many=True, allow_none=False)
    pagination = ma_fields.Nested(PivotPaginationSchema, load_default=PivotPaginationSpec())
    with_totals = ma_fields.Boolean(allow_none=True, load_default=None)
    totals = ma_fields.Nested(PivotTotalsSchema, allow_none=True, load_default=None)


class PivotHeaderInfoSchema(DefaultSchema[PivotHeaderInfo]):
    TARGET_CLS = PivotHeaderInfo

    sorting_direction = EnumField(OrderDirection, allow_none=True)
    role_spec = ma_fields.Nested(PivotHeaderRoleSpecSchema, allow_none=False, required=True)
