from __future__ import annotations

from marshmallow import fields as ma_fields, post_dump
from typing import Any, Dict

from bi_constants.enums import ManagedBy

from bi_model_tools.schema.base import DefaultSchema, DefaultValidateSchema

from bi_api_lib.request_model.data import AddUpdateObligatoryFilter
from bi_api_lib.enums import WhereClauseOperation
from bi_api_lib.query.formalization.raw_specs import IdOrTitleFieldRef, RawFilterFieldSpec


class WhereSchema(DefaultSchema[RawFilterFieldSpec]):
    TARGET_CLS = RawFilterFieldSpec

    column = ma_fields.String(required=True)
    operation = ma_fields.Enum(WhereClauseOperation, required=True)
    values = ma_fields.List(ma_fields.Raw(allow_none=True), required=True, allow_none=True)

    def to_object(self, data: dict) -> RawFilterFieldSpec:
        return RawFilterFieldSpec(
            ref=IdOrTitleFieldRef(id_or_title=data['column']),
            operation=data['operation'],
            values=data['values'],
        )


class ObligatoryFilterSchema(DefaultValidateSchema[AddUpdateObligatoryFilter]):
    TARGET_CLS = AddUpdateObligatoryFilter

    id = ma_fields.String(required=True)
    field_guid = ma_fields.String()
    default_filters = ma_fields.Nested(WhereSchema, many=True, load_default=[])
    managed_by = ma_fields.Enum(ManagedBy, allow_none=True, dump_default=ManagedBy.user, load_default=ManagedBy.user)
    valid = ma_fields.Boolean(load_default=True)

    @post_dump
    def set_where_column(self, data: Dict[str, Any], **_: Any) -> Dict[str, Any]:
        if data.get('field_guid'):
            for filter in data['default_filters']:
                if not filter.get('column'):
                    filter['column'] = data['field_guid']
        return data
