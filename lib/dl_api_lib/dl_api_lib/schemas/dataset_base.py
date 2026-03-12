from __future__ import annotations

from copy import deepcopy
from typing import Any
import uuid

from marshmallow import (
    post_dump,
    post_load,
    pre_load,
    validates_schema,
)
from marshmallow import ValidationError
from marshmallow import fields as ma_fields
from marshmallow_oneofschema import OneOfSchema

from dl_api_connector.api_schema.component_errors import ComponentErrorListSchema
from dl_api_connector.api_schema.source import DataSourceStrictSchema
from dl_api_connector.api_schema.source_base import (
    AvatarRelationSchema,
    SourceAvatarStrictSchema,
    VirtualFlagField,
)
from dl_api_connector.api_schema.top_level import USEntryAnnotationMixin
from dl_api_lib.schemas.fields import ResultSchemaAuxSchema
from dl_api_lib.schemas.filter import ObligatoryFilterSchema
from dl_api_lib.schemas.options import OptionsMixin
from dl_api_lib.schemas.parameters import ParameterValueConstraintSchema
from dl_constants.enums import (
    AggregationFunction,
    CacheInvalidationMode,
    CalcMode,
    FieldType,
    ManagedBy,
    NotificationLevel,
    RLSPatternType,
    RLSSubjectType,
    UserDataType,
)
from dl_core.base_models import (
    CacheInvalidationError,
    CacheInvalidationSource,
)
from dl_core.fields import (
    BIField,
    CalculationSpec,
    DirectCalculationSpec,
    FormulaCalculationSpec,
    ParameterCalculationSpec,
    del_calc_spec_kwargs_from,
    filter_calc_spec_kwargs,
)
from dl_model_tools.schema.base import (
    BaseSchema,
    DefaultSchema,
)
from dl_model_tools.schema.typed_values import (
    ValueSchema,
    WithNestedValueSchema,
)
from dl_rls.models import (
    RLSEntry,
    RLSSubject,
)


class DirectCalculationSpecSchema(DefaultSchema[DirectCalculationSpec]):
    TARGET_CLS = DirectCalculationSpec

    avatar_id = ma_fields.String(allow_none=True)
    source = ma_fields.String()


class FormulaCalculationSpecSchema(DefaultSchema[FormulaCalculationSpec]):
    TARGET_CLS = FormulaCalculationSpec

    formula = ma_fields.String(load_default="")
    guid_formula = ma_fields.String(load_default="")


class ParameterCalculationSpecSchema(DefaultSchema[ParameterCalculationSpec]):
    TARGET_CLS = ParameterCalculationSpec

    default_value = ma_fields.Nested(ValueSchema, allow_none=True)
    value_constraint = ma_fields.Nested(ParameterValueConstraintSchema, allow_none=True)
    template_enabled = ma_fields.Bool(load_default=False)


class RLS2ConfigEntrySchema(DefaultSchema[RLSEntry]):
    TARGET_CLS = RLSEntry

    class RLSSubjectSchema(DefaultSchema[RLSSubject]):
        TARGET_CLS = RLSSubject

        subject_id = ma_fields.String(required=True)
        subject_type = ma_fields.Enum(RLSSubjectType)
        subject_name = ma_fields.String(load_default=None, allow_none=True)

    field_guid = ma_fields.String(dump_default=None, load_default=None)
    allowed_value = ma_fields.String(dump_default=None, load_default=None)
    pattern_type = ma_fields.Enum(RLSPatternType, load_default=RLSPatternType.value)
    subject = ma_fields.Nested(RLSSubjectSchema, required=True)


class ResultSchemaBase(DefaultSchema[BIField]):
    title = ma_fields.String(required=True)
    guid = ma_fields.String()
    hidden = ma_fields.Boolean(load_default=False)
    description = ma_fields.String()
    initial_data_type = ma_fields.Enum(UserDataType, allow_none=True)
    cast = ma_fields.Enum(UserDataType)
    type = ma_fields.Enum(FieldType, readonly=True)
    data_type = ma_fields.Enum(UserDataType, allow_none=True)
    valid = ma_fields.Boolean(allow_none=True)
    ui_settings = ma_fields.String(dump_default="", load_default="")
    aggregation = ma_fields.Enum(AggregationFunction, load_default=AggregationFunction.none.name)
    aggregation_locked = ma_fields.Boolean(readonly=True, allow_none=True, load_default=False, dump_only=True)
    autoaggregated = ma_fields.Boolean(readonly=True, allow_none=True, dump_only=True)
    virtual = VirtualFlagField(attribute="managed_by", dump_only=True)
    has_auto_aggregation = ma_fields.Boolean(allow_none=True)
    lock_aggregation = ma_fields.Boolean(allow_none=True)
    managed_by = ma_fields.Enum(ManagedBy, allow_none=True, dump_default=ManagedBy.user)

    @post_dump(pass_many=False)
    def add_calc_spec(self, data: dict[str, Any], **_: Any) -> dict[str, Any]:
        data = deepcopy(data)
        calc_spec_data = data.pop("calc_spec")
        calc_spec_data["calc_mode"] = calc_spec_data.pop("mode")
        data.update(calc_spec_data)
        # For backward compatibility use '' for formula and source; avatar_id must be present even if None
        for key in ("formula", "guid_formula", "source"):
            data.setdefault(key, "")
        for key in ("avatar_id", "default_value", "value_constraint"):
            data.setdefault(key, None)
        return data

    def to_object(self, data: dict) -> BIField:
        return BIField.make(**data)


class ResultSchemaSchema(WithNestedValueSchema, ResultSchemaBase):
    TYPE_FIELD_NAME = "cast"
    TARGET_CLS = BIField

    class CalculationSpecSchema(OneOfSchema):
        type_field = "mode"
        type_schemas = {
            CalcMode.direct.name: DirectCalculationSpecSchema,
            CalcMode.formula.name: FormulaCalculationSpecSchema,
            CalcMode.parameter.name: ParameterCalculationSpecSchema,
        }

        def get_obj_type(self, obj: CalculationSpec) -> str:
            return obj.mode.name

    # this will be flattened on dump and un-flattened before load
    # TODO: dump/load as is and update usage on front end respectively
    calc_spec = ma_fields.Nested(CalculationSpecSchema)

    @pre_load(pass_many=False)
    def extract_calc_spec(self, data: dict[str, Any], **_: Any) -> dict[str, Any]:
        data = deepcopy(data)
        mode = data["calc_mode"]
        data["calc_spec"] = dict(filter_calc_spec_kwargs(mode, data), mode=mode)
        return del_calc_spec_kwargs_from(data)

    @validates_schema
    def check_source_is_set_for_direct_and_aggregation_types(
        self, data: dict[str, Any], *args: Any, **kwargs: Any
    ) -> None:
        if data["calc_spec"].mode == CalcMode.direct.name:
            if not data["calc_spec"].source:
                raise ValidationError("source is required for {}".format(data["title"]))


class CacheInvalidationErrorSchema(DefaultSchema[CacheInvalidationError]):
    """Schema for cache invalidation execution errors"""

    TARGET_CLS = CacheInvalidationError

    title = ma_fields.String()
    message = ma_fields.String()
    level = ma_fields.Enum(NotificationLevel)
    locator = ma_fields.String()


class CacheInvalidationLastResultErrorSchema(BaseSchema):
    """Schema for last cache invalidation execution errors"""

    code = ma_fields.String(required=True)
    message = ma_fields.String(allow_none=True)
    details = ma_fields.Dict(allow_none=True, load_default=dict)
    debug = ma_fields.Dict(allow_none=True, load_default=dict)


class CacheInvalidationFieldSchema(ResultSchemaBase):
    """Schema for cache invalidation field (formula mode).

    Inherits all fields from ResultSchemaBase with overridden ones
    """

    title = ma_fields.String(load_default="INVALIDATION CACHE SERVICE FIELD")

    # this will be flattened on dump and un-flattened before load
    # TODO: dump/load as is and update usage on front end respectively
    calc_spec = ma_fields.Nested(FormulaCalculationSpecSchema)

    @pre_load(pass_many=False)
    def extract_calc_spec(self, data: dict[str, Any], **_: Any) -> dict[str, Any]:
        data = deepcopy(data)
        mode = data.get("calc_mode", CalcMode.formula.name)
        data["calc_spec"] = dict(filter_calc_spec_kwargs(mode, data), mode=mode)
        return del_calc_spec_kwargs_from(data)

    @post_dump(pass_many=False)
    def add_calc_spec(self, data: dict[str, Any], **_: Any) -> dict[str, Any]:
        data = deepcopy(data)
        calc_spec_data = data.pop("calc_spec")
        calc_spec_data["calc_mode"] = calc_spec_data.pop("mode", CalcMode.formula.name)
        data.update(calc_spec_data)
        # For backward compatibility use '' for formula and source; avatar_id must be present even if None
        for key in ("formula", "guid_formula", "source"):
            data.setdefault(key, "")
        for key in ("avatar_id", "default_value", "value_constraint"):
            data.setdefault(key, None)
        return data


class CacheInvalidationSourceSchema(DefaultSchema[CacheInvalidationSource]):
    """Schema for cache_invalidation_source object in dataset"""

    TARGET_CLS = CacheInvalidationSource

    mode = ma_fields.Enum(CacheInvalidationMode, load_default=CacheInvalidationMode.off)

    # For mode: formula
    filters = ma_fields.Nested(
        ObligatoryFilterSchema,
        many=True,
        allow_none=True,
        load_default=None,
    )
    field = ma_fields.Nested(CacheInvalidationFieldSchema, allow_none=True, load_default=None)

    # For mode: sql
    sql = ma_fields.String(allow_none=True, load_default=None)

    # Error fields
    cache_invalidation_error = ma_fields.Nested(
        CacheInvalidationErrorSchema,
        allow_none=True,
        dump_default=None,
        load_default=None,
    )
    last_result_timestamp = ma_fields.String(
        allow_none=True,
        dump_default=None,
        dump_only=True,
    )
    last_result_error = ma_fields.Nested(
        CacheInvalidationLastResultErrorSchema,
        allow_none=True,
        dump_default=None,
        dump_only=True,
    )


class DatasetContentInternalSchema(BaseSchema, USEntryAnnotationMixin):
    """
    A base class for schemas that need to contain the full dataset description
    """

    sources = ma_fields.Nested(DataSourceStrictSchema, many=True, required=False)
    source_avatars = ma_fields.Nested(SourceAvatarStrictSchema, many=True, required=False)
    avatar_relations = ma_fields.Nested(AvatarRelationSchema, many=True, required=False)
    result_schema = ma_fields.List(ma_fields.Nested(ResultSchemaSchema, required=False))
    result_schema_aux = ma_fields.Nested(ResultSchemaAuxSchema, allow_none=False)
    rls = ma_fields.Dict(load_default=dict, required=False)
    rls2 = ma_fields.Dict(
        ma_fields.String,
        ma_fields.List(ma_fields.Nested(RLS2ConfigEntrySchema)),
        required=False,
        load_default=dict,
        dump_default=dict,
    )
    preview_enabled = ma_fields.Boolean(load_default=False)
    component_errors = ma_fields.Nested(ComponentErrorListSchema, required=False)
    obligatory_filters = ma_fields.Nested(ObligatoryFilterSchema, many=True, load_default=list)
    revision_id = ma_fields.String(allow_none=True, dump_default=None, load_default=None)
    load_preview_by_default = ma_fields.Boolean(dump_default=True, load_default=True)
    template_enabled = ma_fields.Boolean(dump_default=False, load_default=False)
    data_export_forbidden = ma_fields.Boolean(dump_default=False, load_default=False)
    cache_invalidation_source = ma_fields.Nested(CacheInvalidationSourceSchema)

    @pre_load
    def prepare_guids(self, in_data: dict[str, Any], *args: Any, **kwargs: Any) -> dict[str, Any]:
        for item in in_data.get("result_schema", ()):
            if not item.get("guid"):
                item["guid"] = str(uuid.uuid4())

        cache_invalidation_source_field = in_data.get("cache_invalidation_source", {}).get("field")
        if cache_invalidation_source_field and not cache_invalidation_source_field.get("guid"):
            cache_invalidation_source_field["guid"] = str(uuid.uuid4())

        return in_data

    @pre_load
    def check_rls(self, in_data: dict[str, Any], *args: Any, **kwargs: Any) -> dict[str, Any]:
        if in_data.get("rls") and in_data.get("rls2"):
            raise ValidationError("RLS can be specified in only one of the two fields: rls or rls2")
        return in_data

    @post_load
    def validate_and_postload_rls2(self, item: dict[str, Any], *args: Any, **kwargs: Any) -> dict[str, Any]:
        for key, entries in item["rls2"].items():
            for entry in entries:
                if entry.pattern_type == RLSPatternType.value and entry.allowed_value is None:
                    raise ValidationError(
                        "RLS validation error: allowed_value must be not None for 'value' RLS pattern type"
                    )
                if entry.pattern_type != RLSPatternType.value and entry.allowed_value is not None:
                    raise ValidationError(
                        f"RLS validation error: allowed_value must be None for '{entry.pattern_type}' RLS pattern type"
                    )
                entry.field_guid = key
                entry.subject.subject_name = entry.subject.subject_name or entry.subject.subject_id
        return item


class DatasetContentSchema(OptionsMixin):
    dataset = ma_fields.Nested(DatasetContentInternalSchema, required=True)
