from __future__ import annotations

from enum import Enum
import json
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
)

from marshmallow import (
    pre_dump,
    pre_load,
)
from marshmallow import EXCLUDE
from marshmallow import fields as ma_fields
from marshmallow_oneofschema import OneOfSchema

from dl_api_lib.query.formalization.raw_pivot_specs import RawPivotSpec
from dl_api_lib.query.formalization.raw_specs import (
    DimensionNameRef,
    IdFieldRef,
    IdOrTitleFieldRef,
    MeasureNameRef,
    PlaceholderRef,
    RawAfterBlockPlacement,
    RawBlockSpec,
    RawDimensionValueSpec,
    RawFilterFieldSpec,
    RawGroupByFieldSpec,
    RawOrderByFieldSpec,
    RawOrderByRoleSpec,
    RawParameterValueSpec,
    RawQueryMetaInfo,
    RawQuerySpecUnion,
    RawRangeRoleSpec,
    RawResultSpec,
    RawRoleSpec,
    RawRootBlockPlacement,
    RawRowRoleSpec,
    RawSelectFieldSpec,
    RawTemplateRoleSpec,
    RawTreeRoleSpec,
    TitleFieldRef,
)
from dl_api_lib.request_model.data import (
    DataRequestModel,
    PivotDataRequestModel,
    ResultDataRequestModel,
)
from dl_api_lib.schemas.action import ActionSchema
from dl_api_lib.schemas.dataset_base import DatasetContentSchema
from dl_api_lib.schemas.filter import WhereSchema
from dl_api_lib.schemas.legend import LegendItemSchema
from dl_api_lib.schemas.pivot import RequestPivotSpecSchema
from dl_constants.enums import (
    CalcMode,
    FieldRole,
    FieldType,
    FieldVisibility,
    NotificationLevel,
    OrderDirection,
    QueryBlockPlacementType,
    QueryItemRefType,
    RangeType,
    UserDataType,
    WhereClauseOperation,
)
from dl_core.constants import DataAPILimits
from dl_model_tools.schema.base import (
    BaseSchema,
    DefaultSchema,
)
from dl_query_processing.enums import (
    GroupByPolicy,
    QueryType,
)
from dl_query_processing.legend.block_legend import BlockPlacement


class DatasetDataRequestBaseSchema(DefaultSchema[DataRequestModel]):
    TARGET_CLS = DataRequestModel

    def to_object(self, data: dict) -> DataRequestModel:
        raw_query_spec_union = self._make_raw_query_spec_union(data)
        drm = self._make_drm(raw_query_spec_union=raw_query_spec_union, data=data)
        return drm

    def _make_raw_query_spec_union(self, data: Dict[str, Any]) -> RawQuerySpecUnion:
        raise NotImplementedError

    def _make_drm(self, raw_query_spec_union: RawQuerySpecUnion, data: Dict[str, Any]) -> DataRequestModel:
        raise NotImplementedError


class DatasetPreviewRequestSchema(DatasetDataRequestBaseSchema, DatasetContentSchema):
    limit = ma_fields.Integer(load_default=100)
    row_count_hard_limit = ma_fields.Integer(load_default=DataAPILimits.PREVIEW_API_DEFAULT_ROW_COUNT_HARD_LIMIT)
    updates = ma_fields.Nested(ActionSchema, many=True, load_default=[])
    TARGET_CLS = DataRequestModel

    def _make_raw_query_spec_union(self, data: Dict[str, Any]) -> RawQuerySpecUnion:
        raw_query_spec_union = RawQuerySpecUnion(
            limit=data.get("limit"),
            group_by_policy=GroupByPolicy.if_measures,
            meta=RawQueryMetaInfo(
                query_type=QueryType.preview,
                row_count_hard_limit=data["row_count_hard_limit"],
            ),
        )
        return raw_query_spec_union

    def _make_drm(self, raw_query_spec_union: RawQuerySpecUnion, data: Dict[str, Any]) -> DataRequestModel:
        return DataRequestModel(
            raw_query_spec_union=raw_query_spec_union,
            dataset=data.get("dataset"),
            updates=data["updates"],
        )


class FieldsResponseFieldSchema(BaseSchema):
    title = ma_fields.String()
    guid = ma_fields.String()
    data_type = ma_fields.Enum(UserDataType)
    hidden = ma_fields.Boolean()
    type = ma_fields.Enum(FieldType)
    calc_mode = ma_fields.Enum(CalcMode)


class DatasetFieldsResponseSchema(BaseSchema):
    fields_ = ma_fields.Nested(FieldsResponseFieldSchema, attribute="fields", data_key="fields", many=True)


class YqlDataSchema(BaseSchema):
    Type = ma_fields.Raw()
    Data = ma_fields.List(ma_fields.List(ma_fields.Raw()))


class OrderBySchema(DefaultSchema[RawOrderByFieldSpec]):
    class ApiOrderingDirection(Enum):
        DESC = "desc"
        ASC = "asc"

    TARGET_CLS = RawOrderByFieldSpec

    column = ma_fields.String(required=True)
    direction = ma_fields.Enum(ApiOrderingDirection, required=True)

    def to_object(self, data: dict) -> RawOrderByFieldSpec:
        data["direction"] = {
            self.ApiOrderingDirection.ASC: OrderDirection.asc,
            self.ApiOrderingDirection.DESC: OrderDirection.desc,
        }[data["direction"]]
        data["ref"] = IdOrTitleFieldRef(id_or_title=data.pop("column"))
        return self.get_target_cls()(**data)


class DatasetVersionResultRequestSchema(DatasetDataRequestBaseSchema):
    columns = ma_fields.List(ma_fields.String(), required=True)
    where = ma_fields.Nested(WhereSchema, many=True, load_default=[])
    group_by = ma_fields.List(ma_fields.String(), load_default=[])
    order_by = ma_fields.Nested(OrderBySchema, many=True, load_default=[])
    limit = ma_fields.Integer(load_default=None)
    offset = ma_fields.Integer(load_default=None)
    updates = ma_fields.Nested(ActionSchema, many=True, load_default=[])
    row_count_hard_limit = ma_fields.Integer(load_default=DataAPILimits.DATA_API_DEFAULT_ROW_COUNT_HARD_LIMIT)
    with_totals = ma_fields.Boolean(load_default=False)
    disable_group_by = ma_fields.Boolean(load_default=False)
    add_fields_data = ma_fields.Boolean(load_default=True)
    ignore_nonexistent_filters = ma_fields.Boolean(load_default=False)
    revision_id = ma_fields.String(load_default=None)

    def _make_raw_query_spec_union(self, data: Dict[str, Any]) -> RawQuerySpecUnion:
        raw_query_spec_union = RawQuerySpecUnion(
            select_specs=[RawSelectFieldSpec(ref=IdOrTitleFieldRef(id_or_title=field)) for field in data["columns"]],
            group_by_specs=[
                RawGroupByFieldSpec(ref=IdOrTitleFieldRef(id_or_title=field)) for field in data["group_by"]
            ],
            filter_specs=[
                RawFilterFieldSpec(ref=filter_spec.ref, operation=filter_spec.operation, values=filter_spec.values)
                for filter_spec in data["where"]
            ],
            order_by_specs=data["order_by"],
            limit=data["limit"],
            offset=data["offset"],
            group_by_policy=GroupByPolicy.disable if data.get("disable_group_by") else GroupByPolicy.force,
            ignore_nonexistent_filters=data["ignore_nonexistent_filters"],
            meta=RawQueryMetaInfo(
                query_type=QueryType.result,
                row_count_hard_limit=data["row_count_hard_limit"],
            ),
        )
        return raw_query_spec_union

    def _make_drm(self, raw_query_spec_union: RawQuerySpecUnion, data: Dict[str, Any]) -> DataRequestModel:
        return DataRequestModel(
            raw_query_spec_union=raw_query_spec_union,
            updates=data["updates"],
            with_totals=data["with_totals"],
            add_fields_data=data["add_fields_data"],
            dataset_revision_id=data.get("revision_id"),
        )


class DatasetVersionResultResponseSchema(BaseSchema):
    class Result(BaseSchema):
        query = ma_fields.String(required=False)
        data = ma_fields.Nested(YqlDataSchema)

        # Rare flag, thus making it possible to not return it on every
        # unrelated request.
        not_aggregated = ma_fields.Boolean(required=False)

        totals_query = ma_fields.String(required=False)
        totals = ma_fields.List(ma_fields.String())

        data_export_forbidden = ma_fields.Boolean(required=False)
        fields_ = ma_fields.Nested(
            FieldsResponseFieldSchema, attribute="fields", data_key="fields", many=True, required=False
        )

    result = ma_fields.Nested(Result)


class DatasetVersionValuesBasePostSchema(DatasetDataRequestBaseSchema):
    QUERY_TYPE: ClassVar[QueryType]

    field_guid = ma_fields.String(required=True)
    where = ma_fields.Nested(WhereSchema, many=True, load_default=[])
    updates = ma_fields.Nested(ActionSchema, many=True, load_default=[])
    revision_id = ma_fields.String(load_default=None)

    def _make_select_specs(self, field_id: str) -> List[RawSelectFieldSpec]:
        return [RawSelectFieldSpec(ref=IdFieldRef(id=field_id))]

    def _make_raw_query_spec_union(self, data: Dict[str, Any]) -> RawQuerySpecUnion:
        raw_query_spec_union = RawQuerySpecUnion(
            select_specs=self._make_select_specs(field_id=data["field_guid"]),
            order_by_specs=[],
            filter_specs=[
                RawFilterFieldSpec(ref=filter_spec.ref, operation=filter_spec.operation, values=filter_spec.values)
                for filter_spec in data["where"]
            ],
            limit=data.get("limit"),
            offset=data.get("offset"),
            group_by_policy=GroupByPolicy.disable,
            disable_rls=data.get("disable_rls", False),
            ignore_nonexistent_filters=data.get("ignore_nonexistent_filters", False),
            allow_measure_fields=False,  # Cannot get distincts or ranges for measures
            meta=RawQueryMetaInfo(
                query_type=self.QUERY_TYPE,
            ),
        )
        return raw_query_spec_union


class DatasetVersionValuesDistinctPostSchema(DatasetVersionValuesBasePostSchema):
    QUERY_TYPE = QueryType.distinct

    row_count_hard_limit = ma_fields.Integer(load_default=DataAPILimits.DATA_API_DEFAULT_ROW_COUNT_HARD_LIMIT)
    limit = ma_fields.Integer(load_default=1000)
    offset = ma_fields.Integer(load_default=0)
    ignore_nonexistent_filters = ma_fields.Boolean(load_default=False)
    disable_rls = ma_fields.Boolean(load_default=False)

    def _make_select_specs(self, field_id: str) -> List[RawSelectFieldSpec]:
        # Right now DISTINCT is handled at query level,
        # so the expressions are compiled in the regular manner
        return [RawSelectFieldSpec(ref=IdFieldRef(id=field_id), role_spec=RawRoleSpec(role=FieldRole.distinct))]

    def _make_drm(self, raw_query_spec_union: RawQuerySpecUnion, data: Dict[str, Any]) -> DataRequestModel:
        return DataRequestModel(
            raw_query_spec_union=raw_query_spec_union,
            updates=data["updates"],
            dataset_revision_id=data.get("revision_id"),
        )


class DatasetVersionValuesDistinctResponseSchema(DatasetVersionResultResponseSchema):
    pass


class DatasetVersionValuesRangePostSchema(DatasetVersionValuesBasePostSchema):
    QUERY_TYPE = QueryType.value_range

    def _make_select_specs(self, field_id: str) -> List[RawSelectFieldSpec]:
        return [
            RawSelectFieldSpec(
                ref=IdFieldRef(id=field_id),
                role_spec=RawRangeRoleSpec(role=FieldRole.range, range_type=RangeType.min),
            ),
            RawSelectFieldSpec(
                ref=IdFieldRef(id=field_id),
                role_spec=RawRangeRoleSpec(role=FieldRole.range, range_type=RangeType.max),
            ),
        ]

    def _make_drm(self, raw_query_spec_union: RawQuerySpecUnion, data: Dict[str, Any]) -> DataRequestModel:
        return DataRequestModel(
            raw_query_spec_union=raw_query_spec_union,
            updates=data["updates"],
            dataset_revision_id=data.get("revision_id"),
        )


class DatasetVersionValuesRangeResponseSchema(DatasetVersionResultResponseSchema):
    pass


class IdRefSchema(DefaultSchema[IdFieldRef]):
    TARGET_CLS = IdFieldRef

    id = ma_fields.String(required=True)


class TitleRefSchema(DefaultSchema[TitleFieldRef]):
    TARGET_CLS = TitleFieldRef

    title = ma_fields.String(required=True)


class MeasureNameRefSchema(DefaultSchema[MeasureNameRef]):
    TARGET_CLS = MeasureNameRef


class DimensionNameRefSchema(DefaultSchema[DimensionNameRef]):
    TARGET_CLS = DimensionNameRef


class PlaceholderRefSchema(DefaultSchema[PlaceholderRef]):
    TARGET_CLS = PlaceholderRef


class ItemRefSchema(OneOfSchema):
    class Meta:
        unknown = EXCLUDE

    type_field_remove = True
    type_field = "type"

    type_schemas: Dict[str, Any] = {
        QueryItemRefType.id.name: IdRefSchema,
        QueryItemRefType.title.name: TitleRefSchema,
        QueryItemRefType.measure_name.name: MeasureNameRefSchema,
        QueryItemRefType.dimension_name.name: DimensionNameRefSchema,
        QueryItemRefType.placeholder.name: PlaceholderRefSchema,
    }


class DimensionValueSpecSchema(DefaultSchema[RawDimensionValueSpec]):
    TARGET_CLS = RawDimensionValueSpec

    legend_item_id = ma_fields.Integer(required=True)
    value = ma_fields.Raw(required=True)


class RoleSpecSchema(OneOfSchema):
    class Meta:
        unknown = EXCLUDE

    type_field_remove = False
    type_field = "role"

    class RoleSpecSchemaVariant(DefaultSchema[RawRoleSpec]):
        TARGET_CLS = RawRoleSpec

        role = ma_fields.Enum(FieldRole)

    class OrderByRoleSpecSchemaVariant(DefaultSchema[RawOrderByRoleSpec]):
        TARGET_CLS = RawOrderByRoleSpec

        role = ma_fields.Enum(FieldRole)
        direction = ma_fields.Enum(OrderDirection)

    class RangeRoleSpecSchemaVariant(DefaultSchema[RawRangeRoleSpec]):
        TARGET_CLS = RawRangeRoleSpec

        role = ma_fields.Enum(FieldRole)
        range_type = ma_fields.Enum(RangeType)

    class RowRoleSpecSchemaVariant(DefaultSchema[RawRowRoleSpec]):
        TARGET_CLS = RawRowRoleSpec

        role = ma_fields.Enum(FieldRole)
        visibility = ma_fields.Enum(FieldVisibility)

    class TemplateRoleSpecSchemaVariant(DefaultSchema[RawTemplateRoleSpec]):
        TARGET_CLS = RawTemplateRoleSpec

        role = ma_fields.Enum(FieldRole)
        template = ma_fields.String()
        visibility = ma_fields.Enum(FieldVisibility)

    class TreeRoleSpecSchemaVariant(DefaultSchema[RawTreeRoleSpec]):
        TARGET_CLS = RawTreeRoleSpec

        role = ma_fields.Enum(FieldRole)
        level = ma_fields.Integer(allow_none=False)
        prefix = ma_fields.String(allow_none=False)
        dimension_values = ma_fields.Nested(DimensionValueSpecSchema, many=True, allow_none=False)
        visibility = ma_fields.Enum(FieldVisibility)

        @pre_dump
        def dump_prefix(self, data: dict, *args: Any, **kwargs: Any) -> dict:
            data["prefix"] = json.dumps(data["prefix"])
            return data

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
    }


class _FlattenRefMixin(BaseSchema):
    @pre_load(pass_many=False)
    def nest_ref_obj(self, data: Dict[str, Any], **_: Any) -> Dict[str, Any]:
        if "ref" not in data:
            ref_type = QueryItemRefType[data["ref_type"]]
            ref_data: Dict[str, Any] = {"type": data["ref_type"]}
            if ref_type == QueryItemRefType.id:
                ref_data["id"] = data.get("id")
            elif ref_type == QueryItemRefType.title:
                ref_data["title"] = data.get("title")
            elif ref_type == QueryItemRefType.measure_name:
                pass
            elif ref_type == QueryItemRefType.placeholder:
                pass
            data["ref"] = ref_data
        return data


class QueryFieldsItemSchema(DefaultSchema[RawSelectFieldSpec], _FlattenRefMixin):
    TARGET_CLS = RawSelectFieldSpec

    ref = ma_fields.Nested(ItemRefSchema, allow_none=False)
    legend_item_id = ma_fields.Integer(allow_none=True)
    role_spec = ma_fields.Nested(RoleSpecSchema, allow_none=False)
    label = ma_fields.String(allow_none=True)
    block_id = ma_fields.Integer(allow_none=True)


class QueryOrderByItemSchema(DefaultSchema[RawOrderByFieldSpec], _FlattenRefMixin):
    TARGET_CLS = RawOrderByFieldSpec

    ref = ma_fields.Nested(ItemRefSchema, allow_none=False)
    direction = ma_fields.Enum(OrderDirection, required=True)
    block_id = ma_fields.Integer(allow_none=True)


class QueryFiltersItemSchema(DefaultSchema[RawFilterFieldSpec], _FlattenRefMixin):
    TARGET_CLS = RawFilterFieldSpec

    ref = ma_fields.Nested(ItemRefSchema, allow_none=False)
    operation = ma_fields.Enum(WhereClauseOperation, required=True)
    values = ma_fields.List(ma_fields.Raw(allow_none=True), required=True, allow_none=True)
    block_id = ma_fields.Integer(allow_none=True)


class RootBlockPlacementSchema(DefaultSchema[RawRootBlockPlacement]):
    TARGET_CLS = RawRootBlockPlacement

    type = ma_fields.Enum(QueryBlockPlacementType)


class AfterBlockPlacementSchema(DefaultSchema[RawAfterBlockPlacement]):
    TARGET_CLS = RawAfterBlockPlacement

    type = ma_fields.Enum(QueryBlockPlacementType)
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


class QueryBlockSchema(DefaultSchema[RawBlockSpec]):
    TARGET_CLS = RawBlockSpec

    block_id = ma_fields.Integer(allow_none=False)
    parent_block_id = ma_fields.Integer(allow_none=True, load_default=None)
    placement = ma_fields.Nested(BlockPlacementSchema, allow_none=False)
    limit = ma_fields.Integer(load_default=None)
    offset = ma_fields.Integer(load_default=None)
    row_count_hard_limit = ma_fields.Integer(load_default=DataAPILimits.DATA_API_DEFAULT_ROW_COUNT_HARD_LIMIT)


class ParameterValueSchema(DefaultSchema[RawParameterValueSpec], _FlattenRefMixin):
    TARGET_CLS = RawParameterValueSpec

    ref = ma_fields.Nested(ItemRefSchema, allow_none=False)
    value = ma_fields.Raw(required=True, allow_none=False)
    block_id = ma_fields.Integer(allow_none=True)


class NewDatasetDataRequestBaseSchema(DatasetDataRequestBaseSchema):
    QUERY_TYPE: ClassVar[QueryType]

    # Query spec
    sel_fields = ma_fields.Nested(
        QueryFieldsItemSchema,
        data_key="fields",
        attribute="fields",
        many=True,
        required=True,
    )
    order_by = ma_fields.Nested(QueryOrderByItemSchema, many=True, load_default=[])
    filters = ma_fields.Nested(QueryFiltersItemSchema, many=True, load_default=[])
    limit = ma_fields.Integer(load_default=None)
    offset = ma_fields.Integer(load_default=None)
    disable_group_by = ma_fields.Boolean(load_default=False)
    row_count_hard_limit = ma_fields.Integer(load_default=DataAPILimits.DATA_API_DEFAULT_ROW_COUNT_HARD_LIMIT)
    ignore_nonexistent_filters = ma_fields.Boolean(load_default=False)
    blocks = ma_fields.Nested(QueryBlockSchema, many=True, load_default=[])
    # Updates
    updates = ma_fields.Nested(ActionSchema, many=True)
    # Misc
    autofill_legend = ma_fields.Boolean(load_default=False)
    revision_id = ma_fields.String(load_default=None)
    # Parameters
    parameter_values = ma_fields.Nested(ParameterValueSchema, many=True, load_default=[])

    def _make_select_specs(self, data: Dict[str, Any]) -> List[RawSelectFieldSpec]:
        return data["fields"]

    def _make_raw_query_spec_union(self, data: Dict[str, Any]) -> RawQuerySpecUnion:
        raw_query_spec_union = RawQuerySpecUnion(
            select_specs=self._make_select_specs(data),
            order_by_specs=data.get("order_by", []),
            filter_specs=data.get("filters", []),
            parameter_value_specs=data.get("parameter_values", []),
            block_specs=data.get("blocks", []),
            limit=data.get("limit"),
            offset=data.get("offset"),
            group_by_policy=GroupByPolicy.disable if data.get("disable_group_by") else GroupByPolicy.force,
            ignore_nonexistent_filters=data["ignore_nonexistent_filters"],
            meta=RawQueryMetaInfo(
                query_type=self.QUERY_TYPE,
                row_count_hard_limit=data["row_count_hard_limit"],
            ),
        )
        return raw_query_spec_union

    def _make_drm(self, raw_query_spec_union: RawQuerySpecUnion, data: Dict[str, Any]) -> DataRequestModel:
        return self.TARGET_CLS(
            raw_query_spec_union=raw_query_spec_union,
            updates=data.get("updates", []),
            autofill_legend=data["autofill_legend"],
            dataset_revision_id=data.get("revision_id"),
        )


class ResultDataRequestV1_5Schema(NewDatasetDataRequestBaseSchema):
    TARGET_CLS = DataRequestModel
    QUERY_TYPE = QueryType.result

    add_fields_data = ma_fields.Boolean(load_default=True)
    with_totals = ma_fields.Boolean(load_default=False)

    def _make_drm(self, raw_query_spec_union: RawQuerySpecUnion, data: Dict[str, Any]) -> DataRequestModel:
        return self.TARGET_CLS(
            raw_query_spec_union=raw_query_spec_union,
            updates=data.get("updates", []),
            autofill_legend=data["autofill_legend"],
            add_fields_data=data["add_fields_data"],
            with_totals=data["with_totals"],
            dataset_revision_id=data.get("revision_id"),
        )


class RequestResultSpecSchema(DefaultSchema[RawResultSpec]):
    TARGET_CLS = RawResultSpec

    with_totals = ma_fields.Boolean(allow_none=False, load_default=False)


class ResultDataRequestV2Schema(NewDatasetDataRequestBaseSchema):
    TARGET_CLS = ResultDataRequestModel
    QUERY_TYPE = QueryType.result

    result = ma_fields.Nested(RequestResultSpecSchema(), allow_none=True)

    def _make_drm(self, raw_query_spec_union: RawQuerySpecUnion, data: Dict[str, Any]) -> ResultDataRequestModel:
        return self.TARGET_CLS(
            raw_query_spec_union=raw_query_spec_union,
            updates=data.get("updates", []),
            result=data.get("result"),
            autofill_legend=data["autofill_legend"],
        )


class DistinctDataRequestV2Schema(NewDatasetDataRequestBaseSchema):
    TARGET_CLS = DataRequestModel
    QUERY_TYPE = QueryType.distinct

    limit = ma_fields.Integer(load_default=1000)  # Define missing value


class RangeDataRequestV2Schema(NewDatasetDataRequestBaseSchema):
    TARGET_CLS = DataRequestModel
    QUERY_TYPE = QueryType.value_range


class PivotDataRequestBaseSchema(NewDatasetDataRequestBaseSchema):
    TARGET_CLS = PivotDataRequestModel
    QUERY_TYPE = QueryType.pivot

    row_count_hard_limit = ma_fields.Integer(
        load_default=DataAPILimits.PIVOT_API_DEFAULT_ROW_COUNT_HARD_LIMIT
    )  # Override default
    pivot = ma_fields.Nested(RequestPivotSpecSchema(), allow_none=False, load_default=lambda: RawPivotSpec())

    def _make_drm(self, raw_query_spec_union: RawQuerySpecUnion, data: Dict[str, Any]) -> DataRequestModel:
        return self.TARGET_CLS(
            raw_query_spec_union=raw_query_spec_union,
            updates=data.get("updates", []),
            pivot=data["pivot"],
            autofill_legend=data["autofill_legend"],
        )


class ResponseBlockInfoSchema(BaseSchema):
    block_id = ma_fields.Integer()
    query = ma_fields.String(allow_none=True)


class V2DataStreamResponseSchema(BaseSchema):
    rows = ma_fields.Raw(allow_none=False)  # Applying schema to data would be very slow


class NotificationSchema(BaseSchema):
    title = ma_fields.String()
    message = ma_fields.String()
    level = ma_fields.Enum(NotificationLevel)
    locator = ma_fields.String()


class DataApiV2ResponseSchema(BaseSchema):
    fields_ = ma_fields.Nested(LegendItemSchema, attribute="fields", data_key="fields", many=True, allow_none=False)
    result_data = ma_fields.Nested(V2DataStreamResponseSchema, many=True, allow_none=False)
    data_export_forbidden = ma_fields.Boolean(allow_none=False)
    blocks = ma_fields.Nested(ResponseBlockInfoSchema, many=True)
    notifications = ma_fields.Nested(NotificationSchema, many=True)
