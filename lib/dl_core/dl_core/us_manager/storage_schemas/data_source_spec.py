from __future__ import annotations

from typing import (
    Any,
)

from marshmallow_oneofschema import OneOfSchema

from dl_constants.enums import DataSourceType
from dl_core.data_source_spec.base import DataSourceSpec
from dl_core.us_manager.storage_schemas.data_source_spec_base import DataSourceSpecStorageSchema


class GenericDataSourceSpecStorageSchema(OneOfSchema):
    type_field = "created_from"
    type_field_remove = False
    type_schemas: dict[str, type[DataSourceSpecStorageSchema]] = {}  # type: ignore  # 2024-01-24 # TODO: Incompatible types in assignment (expression has type "dict[str, type[DataSourceSpecStorageSchema]]", base class "OneOfSchema" defined the type as "dict[str, type[Schema]]")  [assignment]

    def get_obj_type(self, obj: Any) -> str:
        assert isinstance(obj, DataSourceSpec)
        return obj.source_type.name


def register_data_source_schema(source_type: DataSourceType, schema_cls: type[DataSourceSpecStorageSchema]) -> None:
    GenericDataSourceSpecStorageSchema.type_schemas[source_type.name] = schema_cls
