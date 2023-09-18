from typing import (
    Any,
    Optional,
)

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from dl_constants.enums import (
    AggregationFunction,
    BIType,
    CalcMode,
    FieldType,
    ManagedBy,
)

from ..dl_common.base import DatasetAPIBaseModel
from .literals import DefaultValue


@ModelDescriptor()
@attr.s(kw_only=True)
class ResultSchemaField(DatasetAPIBaseModel):
    guid: str = attr.ib()
    strict: bool = attr.ib(default=False)
    title: str = attr.ib()
    source: Optional[str] = attr.ib(default=None)
    calc_mode: CalcMode = attr.ib()
    hidden: bool = attr.ib()
    description: str = attr.ib()
    aggregation: AggregationFunction = attr.ib()
    formula: Optional[str] = attr.ib(default=None)
    guid_formula: Optional[str] = attr.ib(default=None)
    cast: BIType = attr.ib()
    avatar_id: Optional[str] = attr.ib(default=None)
    default_value: Optional[DefaultValue] = attr.ib(default=None)

    # TODO: decide how to represent parameters here
    ignored_keys = {
        "value_constraint",
    }

    @classmethod
    def adopt_json_before_deserialization(cls, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        original_default_value = data.get("default_value")
        normalized_default_value = (
            {
                "type": data["cast"],
                "value": original_default_value,
            }
            if original_default_value is not None
            else None
        )

        return {**data, "default_value": normalized_default_value}

    @classmethod
    def adopt_json_before_sending_to_api(cls, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        if data["guid_formula"] is None:
            del data["guid_formula"]

        orig_default_value = data["default_value"]

        if orig_default_value is not None:
            return {**data, "default_value": orig_default_value["value"]}
        else:
            return data


@ModelDescriptor()
@attr.s(kw_only=True, frozen=True)
class ResultSchemaFieldFull(ResultSchemaField):
    type: FieldType = attr.ib()

    data_type: BIType = attr.ib()
    initial_data_type: BIType = attr.ib()

    has_auto_aggregation: bool = attr.ib()
    lock_aggregation: bool = attr.ib()
    aggregation_locked: bool = attr.ib()
    autoaggregated: bool = attr.ib()

    managed_by: ManagedBy = attr.ib()
    virtual: bool = attr.ib()
    valid: bool = attr.ib()

    def to_writable_result_schema(self) -> ResultSchemaField:
        return ResultSchemaField(
            **{
                key: value
                for key, value in attr.asdict(self, recurse=False).items()
                if key in attr.fields_dict(ResultSchemaField)
            }
        )
