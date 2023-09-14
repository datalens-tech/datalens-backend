from __future__ import annotations

from typing import (
    Any,
    Dict,
)

import attr

from bi_constants.enums import CreateDSFrom
from bi_core.data_source_spec.base import DataSourceSpec
from bi_core.data_source_spec.type_mapping import get_data_source_spec_class


def make_spec_from_dict(source_type: CreateDSFrom, data: Dict[str, Any]) -> DataSourceSpec:
    dsrc_spec_cls = get_data_source_spec_class(ds_type=source_type)
    field_names = {
        field.name.lstrip("_")
        for field in attr.fields(dsrc_spec_cls)
        # Allow only fields with init=True
        if field.init
    }

    filtered_data = {k: v for k, v in data.items() if k in field_names}
    filtered_data["source_type"] = source_type
    return dsrc_spec_cls(**filtered_data)


def update_spec_from_dict(source_type: CreateDSFrom, data: Dict[str, Any], old_spec: DataSourceSpec) -> DataSourceSpec:
    merged_data = {
        # collect old attributes
        field.name.lstrip("_"): getattr(old_spec, field.name)
        for field in attr.fields(type(old_spec))
    }
    merged_data.update(data)
    return make_spec_from_dict(source_type=source_type, data=merged_data)
