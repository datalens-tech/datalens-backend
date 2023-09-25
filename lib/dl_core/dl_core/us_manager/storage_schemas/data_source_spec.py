from __future__ import annotations

from typing import (
    Any,
    Type,
)

from marshmallow_oneofschema import OneOfSchema

from dl_constants.enums import CreateDSFrom
from dl_core.data_source_spec.base import DataSourceSpec
from dl_core.us_manager.storage_schemas.data_source_spec_base import DataSourceSpecStorageSchema


class GenericDataSourceSpecStorageSchema(OneOfSchema):
    type_field = "created_from"
    type_field_remove = False
    type_schemas: dict[str, Type[DataSourceSpecStorageSchema]] = {}

    def get_obj_type(self, obj: Any) -> str:
        assert isinstance(obj, DataSourceSpec)
        return obj.source_type.name


def register_data_source_schema(source_type: CreateDSFrom, schema_cls: Type[DataSourceSpecStorageSchema]) -> None:
    GenericDataSourceSpecStorageSchema.type_schemas[source_type.name] = schema_cls
