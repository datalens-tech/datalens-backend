from typing import (
    Any,
    TypeVar,
)

from marshmallow import EXCLUDE
from marshmallow import fields as ma_fields
from marshmallow_oneofschema import OneOfSchema

from bi_api_client.dsmaker.api.schemas.base import (
    BaseSchema,
    DefaultSchema,
)
from bi_api_client.dsmaker.primitives import (
    AnnotationPivotRoleSpec,
    BlockPlacement,
    DimensionPivotRoleSpec,
    LegendItem,
    OrderByRoleSpec,
    PivotHeaderInfo,
    PivotHeaderRoleSpec,
    PivotHeaderValue,
    PivotItem,
    PivotMeasureRoleSpec,
    PivotMeasureSorting,
    PivotMeasureSortingSettings,
    PivotPagination,
    PivotRoleSpec,
    RangeRoleSpec,
    RequestLegendItemRef,
    RoleSpec,
    RowRoleSpec,
    TemplateRoleSpec,
    TreeRoleSpec,
)
from bi_constants.enums import (
    BIType,
    FieldRole,
    FieldType,
    FieldVisibility,
    LegendItemType,
    NotificationLevel,
    OrderDirection,
    PivotHeaderRole,
    PivotRole,
    QueryBlockPlacementType,
    QueryItemRefType,
    RangeType,
    WhereClauseOperation,
)

_ROLE_SPEC_TV = TypeVar("_ROLE_SPEC_TV", bound=RoleSpec)


class DimensionValueSpecSchema(BaseSchema):
    legend_item_id = ma_fields.Integer()
    value = ma_fields.Raw()


class RoleSpecSchema(OneOfSchema):
    class Meta:
        unknown = EXCLUDE

    type_field_remove = False
    type_field = "role"

    class RoleSpecSchemaVariantBase(DefaultSchema[_ROLE_SPEC_TV]):
        role = ma_fields.Enum(FieldRole)

    class RoleSpecSchemaVariant(RoleSpecSchemaVariantBase[RoleSpec]):
        TARGET_CLS = RoleSpec

    class RowRoleSpecSchemaVariant(RoleSpecSchemaVariantBase[RoleSpec]):
        TARGET_CLS = RowRoleSpec

        visibility = ma_fields.Enum(FieldVisibility)

    class OrderByRoleSpecSchemaVariant(RoleSpecSchemaVariantBase[RoleSpec]):
        TARGET_CLS = OrderByRoleSpec

        direction = ma_fields.Enum(OrderDirection)

    class RangeRoleSpecSchemaVariant(RoleSpecSchemaVariantBase[RoleSpec]):
        TARGET_CLS = RangeRoleSpec

        range_type = ma_fields.Enum(RangeType)

    class TemplateRoleSpecSchemaVariant(RoleSpecSchemaVariantBase[RoleSpec]):
        TARGET_CLS = TemplateRoleSpec

        template = ma_fields.String()
        visibility = ma_fields.Enum(FieldVisibility)

    class TreeRoleSpecSchemaVariant(RoleSpecSchemaVariantBase[RoleSpec]):
        TARGET_CLS = TreeRoleSpec

        level = ma_fields.Integer()
        prefix_req = ma_fields.String(dump_only=True, attribute="prefix", data_key="prefix")  # sent as a string
        prefix_resp = ma_fields.Raw(load_only=True, attribute="prefix", data_key="prefix")  # but returned as an array
        dimension_values = ma_fields.Nested(DimensionValueSpecSchema, many=True, allow_none=True)
        visibility = ma_fields.Enum(FieldVisibility)

    type_schemas = {
        FieldRole.row.name: RowRoleSpecSchemaVariant,
        FieldRole.measure.name: RoleSpecSchemaVariant,
        FieldRole.info.name: RoleSpecSchemaVariant,
        FieldRole.order_by.name: OrderByRoleSpecSchemaVariant,
        FieldRole.distinct.name: RoleSpecSchemaVariant,
        FieldRole.range.name: RangeRoleSpecSchemaVariant,
        FieldRole.total.name: RoleSpecSchemaVariant,
        FieldRole.template.name: TemplateRoleSpecSchemaVariant,
        FieldRole.tree.name: TreeRoleSpecSchemaVariant,
        FieldRole.filter.name: RoleSpecSchemaVariant,
        FieldRole.parameter.name: RoleSpecSchemaVariant,
    }

    def get_obj_type(self, obj: Any) -> str:
        assert isinstance(obj, RoleSpec)
        return obj.role.name


class ItemRefSchema(OneOfSchema):
    class Meta:
        unknown = EXCLUDE

    class RefSchema(BaseSchema):
        type = ma_fields.Enum(QueryItemRefType)

    class IdRefSchema(RefSchema):
        id = ma_fields.String(required=True)

    class TitleRefSchema(RefSchema):
        title = ma_fields.String(required=True)

    class EmptyRefSchema(RefSchema):
        pass

    class PlaceholderRefSchema(RefSchema):
        pass

    type_field_remove = False
    type_field = "type"

    type_schemas = {
        QueryItemRefType.id.name: IdRefSchema,
        QueryItemRefType.title.name: TitleRefSchema,
        QueryItemRefType.measure_name.name: EmptyRefSchema,
        QueryItemRefType.dimension_name.name: EmptyRefSchema,
        QueryItemRefType.placeholder.name: PlaceholderRefSchema,
    }

    def get_obj_type(self, obj: Any) -> str:
        assert isinstance(obj, RequestLegendItemRef)
        return obj.type.name


class QueryFieldsItemSchema(BaseSchema):
    legend_item_id = ma_fields.Integer(allow_none=True)
    block_id = ma_fields.Integer(allow_none=True)
    role_spec = ma_fields.Nested(RoleSpecSchema)
    ref = ma_fields.Nested(ItemRefSchema, allow_none=False)


class FilterClauseSchema(BaseSchema):
    ref = ma_fields.Nested(ItemRefSchema)
    title = ma_fields.String(allow_none=True)
    operation = ma_fields.Enum(WhereClauseOperation)
    values = ma_fields.List(ma_fields.String())
    block_id = ma_fields.Integer(dump_default=None)


class PivotPaginationSchema(DefaultSchema[PivotPagination]):
    TARGET_CLS = PivotPagination

    offset_rows = ma_fields.Integer(allow_none=True)
    limit_rows = ma_fields.Integer(allow_none=True)


class LegendItemSchema(DefaultSchema[LegendItem]):
    TARGET_CLS = LegendItem

    legend_item_id = ma_fields.Integer()
    id = ma_fields.String()
    title = ma_fields.String()
    role_spec = ma_fields.Nested(RoleSpecSchema)
    data_type = ma_fields.Enum(BIType)
    field_type = ma_fields.Enum(FieldType)
    item_type = ma_fields.Enum(LegendItemType)


class ResultSpecSchema(BaseSchema):
    with_totals = ma_fields.Boolean()


class PivotHeaderRoleSpecSchema(DefaultSchema[PivotHeaderRoleSpec]):
    TARGET_CLS = PivotHeaderRoleSpec

    role = ma_fields.Enum(PivotHeaderRole, required=True)


class PivotHeaderValueSchema(DefaultSchema[PivotHeaderValue]):
    TARGET_CLS = PivotHeaderValue

    value = ma_fields.String(allow_none=False, required=True)


class PivotMeasureSortingSettingsSchema(DefaultSchema[PivotMeasureSortingSettings]):
    TARGET_CLS = PivotMeasureSortingSettings

    header_values = ma_fields.List(ma_fields.Nested(PivotHeaderValueSchema), allow_none=False, required=True)
    direction = ma_fields.Enum(OrderDirection, load_default=OrderDirection.asc)
    role_spec = ma_fields.Nested(PivotHeaderRoleSpecSchema, allow_none=False, required=True)


class PivotMeasureSortingSchema(DefaultSchema[PivotMeasureSorting]):
    TARGET_CLS = PivotMeasureSorting

    column = ma_fields.Nested(PivotMeasureSortingSettingsSchema, allow_none=True, load_default=None)
    row = ma_fields.Nested(PivotMeasureSortingSettingsSchema, allow_none=True, load_default=None)


class PivotRoleSpecSchema(OneOfSchema):
    class Meta:
        unknown = EXCLUDE

    type_field_remove = False
    type_field = "role"

    class PivotRoleSpecSchemaVariant(DefaultSchema[PivotRoleSpec]):
        TARGET_CLS = PivotRoleSpec

        role = ma_fields.Enum(PivotRole)

    class DimensionPivotRoleSpecSchemaVariant(DefaultSchema[DimensionPivotRoleSpec]):
        TARGET_CLS = DimensionPivotRoleSpec

        role = ma_fields.Enum(PivotRole)
        direction = ma_fields.Enum(OrderDirection)

    class AnnotationPivotRoleSpecSchemaVariant(DefaultSchema[AnnotationPivotRoleSpec]):
        TARGET_CLS = AnnotationPivotRoleSpec

        role = ma_fields.Enum(PivotRole)
        annotation_type = ma_fields.String()
        target_legend_item_ids = ma_fields.List(ma_fields.Integer(), allow_none=True)

    class MeasurePivotRoleSpecSchemaVariant(DefaultSchema[PivotMeasureRoleSpec]):
        TARGET_CLS = PivotMeasureRoleSpec

        role = ma_fields.Enum(PivotRole, required=True)
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


class PivotItemSchema(DefaultSchema[PivotItem]):
    TARGET_CLS = PivotItem

    pivot_item_id = ma_fields.Integer()
    legend_item_ids = ma_fields.List(ma_fields.Integer())
    role_spec = ma_fields.Nested(PivotRoleSpecSchema)
    title = ma_fields.String()


class PivotTotalItemSchema(BaseSchema):
    level = ma_fields.Integer()


class PivotTotalsSchema(BaseSchema):
    rows = ma_fields.Nested(PivotTotalItemSchema, many=True)
    columns = ma_fields.Nested(PivotTotalItemSchema, many=True)


class PivotSpecSchema(BaseSchema):
    with_totals = ma_fields.Boolean()
    structure = ma_fields.Nested(PivotItemSchema, many=True)
    totals = ma_fields.Nested(PivotTotalsSchema)


class PivotHeaderInfoSchema(DefaultSchema[PivotHeaderInfo]):
    TARGET_CLS = PivotHeaderInfo

    sorting_direction = ma_fields.Enum(OrderDirection, allow_none=True)
    role_spec = ma_fields.Nested(PivotHeaderRoleSpecSchema, allow_none=False, required=True)


class PivotDimensionWithInfoSchema(BaseSchema):
    cells = ma_fields.List(
        ma_fields.List(  # Vector of values (can be None)
            ma_fields.List(ma_fields.Raw()),
            allow_none=True,
        ),
    )
    header_info = ma_fields.Nested(PivotHeaderInfoSchema)


class PivotRowSchema(BaseSchema):
    header = ma_fields.List(  # List of dimensions in row
        ma_fields.List(  # Vector of values (can be None)
            ma_fields.List(ma_fields.Raw()),  # Individual value is a <value, legend_item_id> pair
            allow_none=True,
        ),
    )
    header_with_info = ma_fields.Nested(PivotDimensionWithInfoSchema)
    values = ma_fields.List(  # List of cells in row (per column)
        ma_fields.List(  # Vector of values (can be None)
            ma_fields.List(ma_fields.Raw()),  # Individual value is a <value, legend_item_id> pair
            allow_none=True,
        ),
    )


class PivotDataResponseSchema(BaseSchema):
    columns = ma_fields.List(  # List of columns
        ma_fields.List(  # List of dimensions in column
            ma_fields.List(  # Vector of values (can be None)
                ma_fields.List(ma_fields.Raw()),  # Individual value is a <value, legend_item_id> pair
                allow_none=True,
            ),
        ),
    )
    columns_with_info = ma_fields.List(ma_fields.Nested(PivotDimensionWithInfoSchema))
    row_dimension_headers = ma_fields.List(  # List of row dimension headers
        ma_fields.List(  # Vector of values (can be None)
            ma_fields.List(ma_fields.Raw()),  # Individual value is a <value, legend_item_id> pair
            allow_none=True,
        ),
    )
    rows = ma_fields.Nested(PivotRowSchema, many=True)


class NotificationSchema(BaseSchema):
    title = ma_fields.String()
    message = ma_fields.String()
    level = ma_fields.Enum(NotificationLevel)
    locator = ma_fields.String()


class BlockPlacementSchema(BaseSchema):
    type = ma_fields.Enum(QueryBlockPlacementType)


class RootBlockPlacementSchema(BlockPlacementSchema):
    pass


class AfterBlockPlacementSchema(BlockPlacementSchema):
    dimension_values = ma_fields.Nested(DimensionValueSpecSchema, many=True, allow_none=True)


class BlockPlacementSchema(OneOfSchema):
    class Meta:
        unknown = EXCLUDE

    type_field_remove = True
    type_field = "type"

    type_schemas = {
        QueryBlockPlacementType.root.name: RootBlockPlacementSchema,
        QueryBlockPlacementType.after.name: AfterBlockPlacementSchema,
    }

    def get_obj_type(self, obj: Any) -> str:
        assert isinstance(obj, BlockPlacement)
        return obj.type.name


class QueryBlockSchema(DefaultSchema):
    block_id = ma_fields.Integer(allow_none=False)
    parent_block_id = ma_fields.Integer(allow_none=True, load_default=None)
    placement = ma_fields.Nested(BlockPlacementSchema, allow_none=False)


class BaseDataApiV2ResponseSchema(BaseSchema):
    """
    Base class for Data API v2 responses.
    """

    sel_fields = ma_fields.Nested(LegendItemSchema, data_key="fields", attribute="fields", many=True)
    blocks = ma_fields.Raw()
    notifications = ma_fields.Nested(NotificationSchema, many=True)


class ResultResponseSchema(BaseDataApiV2ResponseSchema):
    """
    Generic v2 response.
    Mainly for the /result endpoint.
    """

    result_data = ma_fields.Raw()


class PivotResponseSchema(BaseDataApiV2ResponseSchema):
    """
    /pivot-specific schema
    """

    pivot_data = ma_fields.Nested(PivotDataResponseSchema)
    pivot = ma_fields.Nested(PivotSpecSchema)
