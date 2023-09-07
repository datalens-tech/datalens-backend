from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional, Sequence, AbstractSet, Type, Union

from marshmallow import EXCLUDE, ValidationError
from marshmallow_oneofschema import OneOfSchema

from bi_constants.enums import CreateDSFrom

from bi_api_connector.api_schema.source_base import (
    DataSourceBaseSchema,
    DataSourceTemplateBaseSchema,
)


class DataSourceSchema(OneOfSchema):
    class Meta:
        unknown = EXCLUDE

    type_field_remove = False
    type_field = 'source_type'
    type_schemas: dict[str, Type[DataSourceBaseSchema]] = {}

    def get_obj_type(self, obj: dict[str, Any]) -> str:
        return obj[self.type_field].name


class DataSourceStrictSchema(DataSourceSchema):
    def load(
        self, data: Union[Mapping[str, Any], Iterable[Mapping[str, Any]]], *, many: Optional[bool] = None,
        partial: Optional[Union[bool, Sequence[str], AbstractSet[str]]] = None,
        unknown: Optional[str] = None, **kwargs: Any
    ) -> Any:
        assert isinstance(data, (dict, list))
        many = many if many is not None else isinstance(data, list)
        if (
            many and any('managed_by' not in source for source in data) or
            not many and 'managed_by' not in data
        ):
            raise ValidationError('Missing `managed_by`')
        return super().load(data, many=many, partial=partial, unknown=unknown, **kwargs)


class DataSourceTemplateResponseSchema(OneOfSchema):
    class Meta:
        unknown = EXCLUDE

    type_field_remove = False
    type_field = 'source_type'
    type_schemas: dict[str, Type[DataSourceTemplateBaseSchema]] = {}

    def get_obj_type(self, obj: dict[str, Any]) -> str:
        return obj[self.type_field].name


def register_source_api_schema(source_type: CreateDSFrom, schema_cls: Type[DataSourceBaseSchema]) -> None:
    DataSourceSchema.type_schemas[source_type.name] = schema_cls


def register_source_template_api_schema(
        source_type: CreateDSFrom, schema_cls: Type[DataSourceTemplateBaseSchema]
) -> None:
    DataSourceTemplateResponseSchema.type_schemas[source_type.name] = schema_cls
