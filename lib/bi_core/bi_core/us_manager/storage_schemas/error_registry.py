from __future__ import annotations

from marshmallow import fields as ma_fields
from marshmallow_oneofschema import OneOfSchema

from bi_constants.enums import ComponentErrorLevel, ComponentType

from bi_core.us_manager.storage_schemas.base import DefaultStorageSchema
from bi_core import component_errors


class GenericComponentErrorPackSchema(DefaultStorageSchema):
    TARGET_CLS = component_errors.ComponentErrorPack

    class GenericComponentErrorSchema(DefaultStorageSchema):
        TARGET_CLS = component_errors.ComponentError

        message = ma_fields.String()
        level = ma_fields.Enum(ComponentErrorLevel)
        code = ma_fields.List(ma_fields.String())
        details = ma_fields.Dict()

    id = ma_fields.String()
    type = ma_fields.Enum(ComponentType)
    errors = ma_fields.List(ma_fields.Nested(GenericComponentErrorSchema))


class ComponentErrorPackSchema(OneOfSchema):
    type_field = 'type'
    type_field_remove = False
    type_schemas = {
        k.name: v for k, v in {
            ComponentType.data_source: GenericComponentErrorPackSchema,
            ComponentType.source_avatar: GenericComponentErrorPackSchema,
            ComponentType.avatar_relation: GenericComponentErrorPackSchema,
            ComponentType.field: GenericComponentErrorPackSchema,
            ComponentType.obligatory_filter: GenericComponentErrorPackSchema,
        }.items()
    }

    def get_obj_type(self, obj):  # type: ignore  # TODO: fix
        return getattr(obj, self.type_field).name


class ComponentErrorListSchema(DefaultStorageSchema):
    TARGET_CLS = component_errors.ComponentErrorRegistry

    items = ma_fields.List(ma_fields.Nested(ComponentErrorPackSchema))
