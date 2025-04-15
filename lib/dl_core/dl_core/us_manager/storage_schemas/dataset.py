from __future__ import annotations

from copy import deepcopy
from typing import (
    Any,
    Dict,
)

from marshmallow import (
    post_dump,
    post_load,
    pre_load,
)
from marshmallow import fields as ma_fields
from marshmallow_oneofschema import OneOfSchema

from dl_constants.enums import (
    AggregationFunction,
    BinaryJoinOperator,
    CalcMode,
    ConditionPartCalcMode,
    FieldType,
    JoinConditionType,
    JoinType,
    ManagedBy,
    ParameterValueConstraintType,
    UserDataType,
    WhereClauseOperation,
)
from dl_core import multisource
from dl_core.base_models import (
    DefaultWhereClause,
    ObligatoryFilter,
)
from dl_core.components.dependencies.primitives import (
    FieldInterDependencyInfo,
    FieldInterDependencyItem,
)
from dl_core.fields import (
    BaseParameterValueConstraint,
    BIField,
    CalculationSpec,
    CollectionParameterValueConstraint,
    DefaultParameterValueConstraint,
    DirectCalculationSpec,
    EqualsParameterValueConstraint,
    FormulaCalculationSpec,
    NotEqualsParameterValueConstraint,
    NullParameterValueConstraint,
    ParameterCalculationSpec,
    RangeParameterValueConstraint,
    RegexParameterValueConstraint,
    ResultSchema,
    SetParameterValueConstraint,
    del_calc_spec_kwargs_from,
    filter_calc_spec_kwargs,
)
from dl_core.us_dataset import Dataset
from dl_core.us_manager.storage_schemas.base import DefaultStorageSchema
from dl_core.us_manager.storage_schemas.data_source_collection import DataSourceCollectionSpecStorageSchema
from dl_core.us_manager.storage_schemas.error_registry import ComponentErrorListSchema
from dl_model_tools.typed_values import (
    ArrayFloatValue,
    ArrayIntValue,
    ArrayStrValue,
    BIValue,
    BooleanValue,
    DateTimeTZValue,
    DateTimeValue,
    DateValue,
    FloatValue,
    GenericDateTimeValue,
    GeoPointValue,
    GeoPolygonValue,
    IntegerValue,
    MarkupValue,
    StringValue,
    TreeStrValue,
    UuidValue,
)
from dl_rls.schema import RLSSchema


class SourceAvatarSchema(DefaultStorageSchema):
    TARGET_CLS = multisource.SourceAvatar

    id = ma_fields.String()
    title = ma_fields.String()
    source_id = ma_fields.String()
    is_root = ma_fields.Boolean()
    managed_by = ma_fields.Enum(ManagedBy, allow_none=True, dump_default=ManagedBy.user)
    valid = ma_fields.Boolean(allow_none=True)


class ConditionPartBaseSchema(DefaultStorageSchema):
    calc_mode = ma_fields.Enum(ConditionPartCalcMode)


class ConditionPartDirectSchema(ConditionPartBaseSchema):
    TARGET_CLS = multisource.ConditionPartDirect
    source = ma_fields.String()


class ConditionPartResultFieldSchema(ConditionPartBaseSchema):
    TARGET_CLS = multisource.ConditionPartResultField
    field_id = ma_fields.String()


class ConditionPartFormulaSchema(ConditionPartBaseSchema):
    TARGET_CLS = multisource.ConditionPartFormula
    formula = ma_fields.String()


class ConditionPartSchema(OneOfSchema):
    type_field = "calc_mode"
    type_field_remove = False
    type_schemas = {
        k.name: v
        for k, v in {
            ConditionPartCalcMode.direct: ConditionPartDirectSchema,
            ConditionPartCalcMode.result_field: ConditionPartResultFieldSchema,
            ConditionPartCalcMode.formula: ConditionPartFormulaSchema,
        }.items()
    }

    def get_obj_type(self, obj):  # type: ignore  # TODO: fix
        return getattr(obj, self.type_field).name


class AvatarRelationSchema(DefaultStorageSchema):
    TARGET_CLS = multisource.AvatarRelation

    class JoinConditionSchema(DefaultStorageSchema):
        TARGET_CLS = multisource.BinaryCondition

        operator = ma_fields.Enum(BinaryJoinOperator)
        left_part = ma_fields.Nested(ConditionPartSchema)
        right_part = ma_fields.Nested(ConditionPartSchema)
        condition_type = ma_fields.Enum(JoinConditionType)

    id = ma_fields.String()
    left_avatar_id = ma_fields.String()
    right_avatar_id = ma_fields.String()
    conditions = ma_fields.List(ma_fields.Nested(JoinConditionSchema))
    join_type = ma_fields.Enum(JoinType)
    managed_by = ma_fields.Enum(ManagedBy, allow_none=True, dump_default=ManagedBy.user)
    valid = ma_fields.Boolean(allow_none=True)
    required = ma_fields.Boolean(load_default=False, dump_default=False)


class BaseValueSchema(DefaultStorageSchema):
    type = ma_fields.Enum(UserDataType)
    value = ma_fields.Field()


class StringValueSchema(BaseValueSchema):
    TARGET_CLS = StringValue
    value = ma_fields.String()


class IntegerValueSchema(BaseValueSchema):
    TARGET_CLS = IntegerValue
    value = ma_fields.Integer()


class FloatValueSchema(BaseValueSchema):
    TARGET_CLS = FloatValue
    value = ma_fields.Float()


class DateValueSchema(BaseValueSchema):
    TARGET_CLS = DateValue
    value = ma_fields.Date()


class DateTimeValueSchema(BaseValueSchema):
    TARGET_CLS = DateTimeValue
    value = ma_fields.DateTime()


class DateTimeTZValueSchema(BaseValueSchema):
    TARGET_CLS = DateTimeTZValue
    value = ma_fields.DateTime()


class GenericDateTimeValueSchema(BaseValueSchema):
    TARGET_CLS = GenericDateTimeValue
    value = ma_fields.DateTime()


class BooleanValueSchema(BaseValueSchema):
    TARGET_CLS = BooleanValue
    value = ma_fields.Boolean()


class GeoPointValueSchema(BaseValueSchema):
    TARGET_CLS = GeoPointValue
    value = ma_fields.List(ma_fields.Float())


class GeoPolygonValueSchema(BaseValueSchema):
    TARGET_CLS = GeoPolygonValue
    value = ma_fields.List(ma_fields.List(ma_fields.List(ma_fields.Float())))


class UuidValueSchema(BaseValueSchema):
    TARGET_CLS = UuidValue
    value = ma_fields.String()


class MarkupValueSchema(BaseValueSchema):
    TARGET_CLS = MarkupValue
    value = ma_fields.String()


class ArrayStrValueSchema(BaseValueSchema):
    TARGET_CLS = ArrayStrValue
    value = ma_fields.List(ma_fields.String())


class ArrayIntValueSchema(BaseValueSchema):
    TARGET_CLS = ArrayIntValue
    value = ma_fields.List(ma_fields.Integer())


class ArrayFloatValueSchema(BaseValueSchema):
    TARGET_CLS = ArrayFloatValue
    value = ma_fields.List(ma_fields.Float())


class TreeStrValueSchema(BaseValueSchema):
    TARGET_CLS = TreeStrValue
    value = ma_fields.List(ma_fields.String())


class ValueSchema(OneOfSchema):
    type_field = "type"
    type_schemas = {
        UserDataType.string.name: StringValueSchema,
        UserDataType.integer.name: IntegerValueSchema,
        UserDataType.float.name: FloatValueSchema,
        UserDataType.date.name: DateValueSchema,
        UserDataType.datetime.name: DateTimeValueSchema,
        UserDataType.datetimetz.name: DateTimeTZValueSchema,
        UserDataType.genericdatetime.name: GenericDateTimeValueSchema,
        UserDataType.boolean.name: BooleanValueSchema,
        UserDataType.geopoint.name: GeoPointValueSchema,
        UserDataType.geopolygon.name: GeoPolygonValueSchema,
        UserDataType.uuid.name: UuidValueSchema,
        UserDataType.markup.name: MarkupValueSchema,
        UserDataType.array_str.name: ArrayStrValueSchema,
        UserDataType.array_int.name: ArrayIntValueSchema,
        UserDataType.array_float.name: ArrayFloatValueSchema,
        UserDataType.tree_str.name: TreeStrValueSchema,
    }

    def get_obj_type(self, obj: BIValue) -> str:
        assert isinstance(obj, BIValue)
        return obj.type.name


class BaseParameterValueConstraintSchema(DefaultStorageSchema):
    type = ma_fields.Enum(ParameterValueConstraintType)


class NullParameterValueConstraintSchema(BaseParameterValueConstraintSchema):
    TARGET_CLS = NullParameterValueConstraint


class RangeParameterValueConstraintSchema(BaseParameterValueConstraintSchema):
    TARGET_CLS = RangeParameterValueConstraint

    min = ma_fields.Nested(ValueSchema, allow_none=True)
    max = ma_fields.Nested(ValueSchema, allow_none=True)


class SetParameterValueConstraintSchema(BaseParameterValueConstraintSchema):
    TARGET_CLS = SetParameterValueConstraint

    values = ma_fields.List(ma_fields.Nested(ValueSchema))


class EqualsParameterValueConstraintSchema(BaseParameterValueConstraintSchema):
    TARGET_CLS = EqualsParameterValueConstraint

    value = ma_fields.Nested(ValueSchema)


class NotEqualsParameterValueConstraintSchema(BaseParameterValueConstraintSchema):
    TARGET_CLS = NotEqualsParameterValueConstraint

    value = ma_fields.Nested(ValueSchema)


class RegexParameterValueConstraintSchema(BaseParameterValueConstraintSchema):
    TARGET_CLS = RegexParameterValueConstraint

    pattern = ma_fields.String()


class DefaultParameterValueConstraintSchema(BaseParameterValueConstraintSchema):
    TARGET_CLS = DefaultParameterValueConstraint


class CollectionParameterValueConstraintSchema(BaseParameterValueConstraintSchema):
    TARGET_CLS = CollectionParameterValueConstraint

    # using lambda to avoid circular import in recursive schema
    constraints = ma_fields.List(ma_fields.Nested(lambda: ParameterValueConstraintSchema()))


class ParameterValueConstraintSchema(OneOfSchema):
    type_field = "type"
    type_schemas = {
        ParameterValueConstraintType.null.name: NullParameterValueConstraintSchema,
        ParameterValueConstraintType.range.name: RangeParameterValueConstraintSchema,
        ParameterValueConstraintType.set.name: SetParameterValueConstraintSchema,
        ParameterValueConstraintType.equals.name: EqualsParameterValueConstraintSchema,
        ParameterValueConstraintType.not_equals.name: NotEqualsParameterValueConstraintSchema,
        ParameterValueConstraintType.regex.name: RegexParameterValueConstraintSchema,
        ParameterValueConstraintType.default.name: DefaultParameterValueConstraintSchema,
        ParameterValueConstraintType.collection.name: CollectionParameterValueConstraintSchema,
    }

    def get_obj_type(self, obj: BaseParameterValueConstraint) -> str:
        return getattr(obj, self.type_field).name


class DirectCalculationSpecSchema(DefaultStorageSchema):
    TARGET_CLS = DirectCalculationSpec
    avatar_id = ma_fields.String(allow_none=True)
    source = ma_fields.String()


class FormulaCalculationSpecSchema(DefaultStorageSchema):
    TARGET_CLS = FormulaCalculationSpec
    formula = ma_fields.String()
    guid_formula = ma_fields.String()


class ParameterCalculationSpecSchema(DefaultStorageSchema):
    TARGET_CLS = ParameterCalculationSpec
    default_value = ma_fields.Nested(ValueSchema, allow_none=True)
    value_constraint = ma_fields.Nested(ParameterValueConstraintSchema, allow_none=True)


class ResultSchemaStorageSchema(DefaultStorageSchema):
    TARGET_CLS = ResultSchema

    class BIFieldSchema(DefaultStorageSchema):
        TARGET_CLS = BIField

        class CalculationSpecSchema(OneOfSchema):
            type_field = "mode"
            type_field_remove = False
            type_schemas = {
                CalcMode.direct.name: DirectCalculationSpecSchema,
                CalcMode.formula.name: FormulaCalculationSpecSchema,
                CalcMode.parameter.name: ParameterCalculationSpecSchema,
            }

            def get_obj_type(self, obj: CalculationSpec) -> str:
                return obj.mode.name

        title = ma_fields.String()
        guid = ma_fields.String()
        aggregation = ma_fields.Enum(AggregationFunction)
        type = ma_fields.Enum(FieldType)
        hidden = ma_fields.Boolean()
        description = ma_fields.String()
        cast = ma_fields.Enum(UserDataType, allow_none=True)
        initial_data_type = ma_fields.Enum(UserDataType, allow_none=True)
        data_type = ma_fields.Enum(UserDataType, allow_none=True)
        valid = ma_fields.Boolean(allow_none=True)
        has_auto_aggregation = ma_fields.Boolean(allow_none=True)
        lock_aggregation = ma_fields.Boolean(allow_none=True)
        managed_by = ma_fields.Enum(ManagedBy, allow_none=True, dump_default=ManagedBy.user)

        # this will be flattened on dump and un-flattened before load
        # TODO: dump/load as is and migrate data in storage respectively
        calc_spec = ma_fields.Nested(CalculationSpecSchema)

        @post_dump(pass_many=False)
        def add_calc_spec(self, data: Dict[str, Any], **_: Any) -> Dict[str, Any]:
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

        @pre_load(pass_many=False)
        def extract_calc_spec(self, data: Dict[str, Any], **_: Any) -> Dict[str, Any]:
            data = deepcopy(data)
            mode = data["calc_mode"]
            data["calc_spec"] = dict(filter_calc_spec_kwargs(mode, data), mode=mode)
            return del_calc_spec_kwargs_from(data)

        def to_object(self, data: dict) -> BIField:
            return BIField.make(**data)

    fields = ma_fields.List(ma_fields.Nested(BIFieldSchema))  # type: ignore  # TODO: fix

    def pre_process_input_data(self, data):  # type: ignore  # TODO: fix
        return {"fields": data}

    @post_dump
    def flatten_fields(self, data, **_):  # type: ignore  # TODO: fix
        return data.get("fields")

    @post_load
    def make_missing_formulas(self, data: Dict[str, Any], **_: Any) -> Dict[str, Any]:
        field: BIField
        titles_to_guids = {field.title: field.guid for field in data["fields"]}
        guids_to_titles = {field.guid: field.title for field in data["fields"]}
        new_fields = []
        for field in data["fields"]:
            if field.calc_mode == CalcMode.formula:
                new_field = field
                if field.formula and not field.guid_formula:
                    new_field = field.clone(
                        guid_formula=BIField.rename_in_formula(formula=field.formula, key_map=titles_to_guids)
                    )
                elif field.guid_formula and not field.formula:
                    new_field = field.clone(
                        formula=BIField.rename_in_formula(formula=field.guid_formula, key_map=guids_to_titles)
                    )
                new_fields.append(new_field)
            else:
                new_fields.append(field)
        data["fields"] = new_fields
        return data


class ResultSchemaAuxSchema(DefaultStorageSchema):
    TARGET_CLS = Dataset.DataModel.ResultSchemaAux

    class FieldInterDependencyInfoSchema(DefaultStorageSchema):
        TARGET_CLS = FieldInterDependencyInfo

        class FieldInterDependencyItemSchema(DefaultStorageSchema):
            TARGET_CLS = FieldInterDependencyItem

            dep_field_id = ma_fields.String(allow_none=False)
            ref_field_ids = ma_fields.List(ma_fields.String(allow_none=False), allow_none=False)

        deps = ma_fields.List(ma_fields.Nested(FieldInterDependencyItemSchema()), allow_none=False)

    inter_dependencies = ma_fields.Nested(FieldInterDependencyInfoSchema(), allow_none=False)


class DefaultWhereClauseSchema(DefaultStorageSchema):
    TARGET_CLS = DefaultWhereClause

    operation = ma_fields.Enum(WhereClauseOperation)
    values = ma_fields.List(ma_fields.String())


class ObligatoryFilterSchema(DefaultStorageSchema):
    TARGET_CLS = ObligatoryFilter

    id = ma_fields.String()
    field_guid = ma_fields.String()
    default_filters = ma_fields.List(ma_fields.Nested(DefaultWhereClauseSchema))
    managed_by = ma_fields.Enum(ManagedBy, allow_none=True, dump_default=ManagedBy.user)
    valid = ma_fields.Boolean(allow_none=True)


class DatasetStorageSchema(DefaultStorageSchema):
    TARGET_CLS = Dataset.DataModel

    name = ma_fields.String(allow_none=True, load_default=None)
    revision_id = ma_fields.String(allow_none=True, dump_default=None, load_default=None)
    load_preview_by_default = ma_fields.Boolean(dump_default=True, load_default=True)
    data_export_forbidden = ma_fields.Boolean(dump_default=False, load_default=False)
    result_schema = ma_fields.Nested(ResultSchemaStorageSchema, allow_none=False)
    result_schema_aux = ma_fields.Nested(ResultSchemaAuxSchema, allow_none=False)
    source_collections = ma_fields.List(ma_fields.Nested(DataSourceCollectionSpecStorageSchema, allow_none=False))
    source_avatars = ma_fields.List(ma_fields.Nested(SourceAvatarSchema, allow_none=False))
    avatar_relations = ma_fields.List(ma_fields.Nested(AvatarRelationSchema, allow_none=False))
    rls = ma_fields.Nested(RLSSchema, allow_none=False)
    component_errors = ma_fields.Nested(ComponentErrorListSchema)
    obligatory_filters = ma_fields.List(ma_fields.Nested(ObligatoryFilterSchema))
    schema_version = ma_fields.String(required=False, allow_none=False, load_default="1", dump_default="1")
