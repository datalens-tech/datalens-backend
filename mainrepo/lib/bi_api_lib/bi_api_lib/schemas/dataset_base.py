from __future__ import annotations

import uuid
from copy import deepcopy
from typing import Any, Dict

from marshmallow import fields as ma_fields, ValidationError, pre_load, post_dump, validates_schema
from marshmallow_oneofschema import OneOfSchema

from bi_constants.enums import (
    AggregationFunction, BIType, CalcMode, FieldType, ManagedBy,
    ComponentErrorLevel, ComponentType,
)

from bi_model_tools.schema.base import BaseSchema, DefaultSchema

from bi_core.fields import (
    BIField, CalculationSpec, DirectCalculationSpec, FormulaCalculationSpec, ParameterCalculationSpec,
    del_calc_spec_kwargs_from, filter_calc_spec_kwargs
)
from bi_core import component_errors
from bi_core.marshmallow import ErrorCodeField

from bi_api_lib.schemas.fields import ResultSchemaAuxSchema
from bi_api_lib.schemas.filter import ObligatoryFilterSchema
from bi_api_lib.schemas.options import OptionsMixin
from bi_api_lib.schemas.parameters import ParameterValueConstraintSchema
from bi_api_connector.api_schema.source import DataSourceStrictSchema
from bi_api_connector.api_schema.source_base import AvatarRelationSchema, SourceAvatarStrictSchema, VirtualFlagField
from bi_api_lib.schemas.values import ValueSchema, WithNestedValueSchema


class DirectCalculationSpecSchema(DefaultSchema[DirectCalculationSpec]):
    TARGET_CLS = DirectCalculationSpec

    avatar_id = ma_fields.String(allow_none=True)
    source = ma_fields.String()


class FormulaCalculationSpecSchema(DefaultSchema[FormulaCalculationSpec]):
    TARGET_CLS = FormulaCalculationSpec

    formula = ma_fields.String(load_default='')
    guid_formula = ma_fields.String(load_default='')


class ParameterCalculationSpecSchema(DefaultSchema[ParameterCalculationSpec]):
    TARGET_CLS = ParameterCalculationSpec

    default_value = ma_fields.Nested(ValueSchema, allow_none=True)
    value_constraint = ma_fields.Nested(ParameterValueConstraintSchema, allow_none=True)


class ResultSchemaSchema(WithNestedValueSchema, DefaultSchema[BIField]):
    TYPE_FIELD_NAME = 'cast'
    TARGET_CLS = BIField

    class CalculationSpecSchema(OneOfSchema):
        type_field = 'mode'
        type_schemas = {
            CalcMode.direct.name: DirectCalculationSpecSchema,
            CalcMode.formula.name: FormulaCalculationSpecSchema,
            CalcMode.parameter.name: ParameterCalculationSpecSchema,
        }

        def get_obj_type(self, obj: CalculationSpec) -> str:
            return obj.mode.name

    title = ma_fields.String(required=True)
    guid = ma_fields.String()
    hidden = ma_fields.Boolean(load_default=False)
    description = ma_fields.String()
    initial_data_type = ma_fields.Enum(BIType, allow_none=True)
    cast = ma_fields.Enum(BIType)
    type = ma_fields.Enum(FieldType, readonly=True)
    data_type = ma_fields.Enum(BIType, allow_none=True)
    valid = ma_fields.Boolean(allow_none=True)

    # this will be flattened on dump and un-flattened before load
    # TODO: dump/load as is and update usage on front end respectively
    calc_spec = ma_fields.Nested(CalculationSpecSchema)

    aggregation = ma_fields.Enum(AggregationFunction, load_default=AggregationFunction.none.name)
    aggregation_locked = ma_fields.Boolean(readonly=True, allow_none=True, load_default=False, dump_only=True)
    autoaggregated = ma_fields.Boolean(readonly=True, allow_none=True, dump_only=True)
    has_auto_aggregation = ma_fields.Boolean(allow_none=True)
    lock_aggregation = ma_fields.Boolean(allow_none=True)

    managed_by = ma_fields.Enum(ManagedBy, allow_none=True, dump_default=ManagedBy.user)
    virtual = VirtualFlagField(attribute='managed_by', dump_only=True)

    @post_dump(pass_many=False)
    def add_calc_spec(self, data: Dict[str, Any], **_: Any) -> Dict[str, Any]:
        data = deepcopy(data)
        calc_spec_data = data.pop('calc_spec')
        calc_spec_data['calc_mode'] = calc_spec_data.pop('mode')
        data.update(calc_spec_data)
        # For backward compatibility use '' for formula and source; avatar_id must be present even if None
        for key in ('formula', 'guid_formula', 'source'):
            data.setdefault(key, '')
        for key in ('avatar_id', 'default_value', 'value_constraint'):
            data.setdefault(key, None)
        return data

    @pre_load(pass_many=False)
    def extract_calc_spec(self, data: Dict[str, Any], **_: Any) -> Dict[str, Any]:
        data = deepcopy(data)
        mode = data['calc_mode']
        data['calc_spec'] = dict(filter_calc_spec_kwargs(mode, data), mode=mode)
        return del_calc_spec_kwargs_from(data)

    @validates_schema
    def check_source_is_set_for_direct_and_aggregation_types(
            self, data: Dict[str, Any], *args: Any, **kwargs: Any
    ) -> None:
        if data['calc_spec'].mode == CalcMode.direct.name:
            if not data['calc_spec'].source:
                raise ValidationError('source is required for {}'.format(data['title']))

    def to_object(self, data: dict) -> BIField:
        return BIField.make(**data)


class ComponentErrorListSchema(DefaultSchema[component_errors.ComponentErrorRegistry]):
    TARGET_CLS = component_errors.ComponentErrorRegistry

    class ComponentErrorPackSchema(DefaultSchema[component_errors.ComponentErrorPack]):
        TARGET_CLS = component_errors.ComponentErrorPack

        class ComponentErrorSchema(DefaultSchema[component_errors.ComponentError]):
            TARGET_CLS = component_errors.ComponentError

            message = ma_fields.String()
            level = ma_fields.Enum(ComponentErrorLevel)
            code = ErrorCodeField()
            details = ma_fields.Dict()

        id = ma_fields.String()
        type = ma_fields.Enum(ComponentType)
        errors = ma_fields.List(ma_fields.Nested(ComponentErrorSchema))

    items = ma_fields.List(ma_fields.Nested(ComponentErrorPackSchema))


class DatasetContentInternalSchema(BaseSchema):
    """
    A base class for schemas that need to contain the full dataset description
    """
    sources = ma_fields.Nested(DataSourceStrictSchema, many=True, required=False)
    source_avatars = ma_fields.Nested(SourceAvatarStrictSchema, many=True, required=False)
    avatar_relations = ma_fields.Nested(AvatarRelationSchema, many=True, required=False)
    result_schema = ma_fields.List(ma_fields.Nested(ResultSchemaSchema, required=False))
    result_schema_aux = ma_fields.Nested(ResultSchemaAuxSchema, allow_none=False)
    rls = ma_fields.Dict(load_default=dict, required=False)
    preview_enabled = ma_fields.Boolean(load_default=False)
    component_errors = ma_fields.Nested(ComponentErrorListSchema, required=False)
    obligatory_filters = ma_fields.Nested(ObligatoryFilterSchema, many=True, load_default=list)
    revision_id = ma_fields.String(allow_none=True, dump_default=None, load_default=None)

    @pre_load
    def prepare_guids(self, in_data: Dict[str, Any], *args: Any, **kwargs: Any) -> Dict[str, Any]:
        for item in in_data.get('result_schema', ()):
            if not item.get('guid'):
                item['guid'] = str(uuid.uuid4())

        return in_data


class DatasetContentSchema(OptionsMixin):
    dataset = ma_fields.Nested(DatasetContentInternalSchema, required=False)
