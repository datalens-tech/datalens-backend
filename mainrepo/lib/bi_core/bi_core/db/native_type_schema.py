"""
Marshmallow schema for the NativeType classes.

Shared between storage (bi_core.us_manager.storage_schemas) and RQE api.

TODO: refactor together with `bi_core.base_models`.
"""

from __future__ import annotations

import logging
from typing import (
    ClassVar,
    Generic,
    Type,
    TypeVar,
)

from marshmallow import (
    Schema,
    fields,
    post_load,
)
from marshmallow_oneofschema import OneOfSchema

from bi_constants.enums import ConnectionType
from bi_model_tools.schema.dynamic_enum_field import DynamicEnumField

from .native_type import (
    ClickHouseDateTime64NativeType,
    ClickHouseDateTime64WithTZNativeType,
    ClickHouseDateTimeWithTZNativeType,
    ClickHouseNativeType,
    CommonNativeType,
    GenericNativeType,
    LengthedNativeType,
)

LOGGER = logging.getLogger(__name__)


_TARGET_TV = TypeVar("_TARGET_TV")


class NativeTypeSchemaBase(Schema, Generic[_TARGET_TV]):
    """(Shared ((Native Type) Storage Schema)), common base class for NT schemas."""

    TARGET_CLS: ClassVar[Type[_TARGET_TV]]

    @post_load(pass_many=False)
    def to_object(self, data: dict, **_):  # type: ignore  # TODO: fix
        return self.TARGET_CLS(**data)  # type: ignore  # TODO: fix


class GenericNativeTypeSchema(NativeTypeSchemaBase[GenericNativeType]):
    """
    ((Generic (Native Type)) Storage Schema),
    (not to be confused with (Generic ((Native Type) Storage Schema))).
    """

    TARGET_CLS = GenericNativeType
    conn_type = DynamicEnumField(ConnectionType)
    name = fields.String()


class CommonNativeTypeSchema(GenericNativeTypeSchema):
    TARGET_CLS = CommonNativeType
    nullable = fields.Boolean(required=False, load_default=True)


class LengthedNativeTypeSchema(CommonNativeTypeSchema):
    TARGET_CLS = LengthedNativeType
    length = fields.Integer(required=False, allow_none=True, load_default=None)


class ClickHouseNativeTypeSchema(CommonNativeTypeSchema):
    TARGET_CLS = ClickHouseNativeType
    lowcardinality = fields.Boolean(required=False, load_default=False)


class ClickHouseDateTimeWithTZNativeTypeSchema(ClickHouseNativeTypeSchema):
    TARGET_CLS = ClickHouseDateTimeWithTZNativeType
    timezone_name = fields.String(required=False, load_default="UTC")
    explicit_timezone = fields.Boolean(required=False, load_default=True)


class ClickHouseDateTime64NativeTypeSchema(ClickHouseNativeTypeSchema):
    TARGET_CLS = ClickHouseDateTime64NativeType  # type: ignore  # TODO: fix
    precision = fields.Integer(required=True)


class ClickHouseDateTime64WithTZNativeTypeSchema(ClickHouseDateTime64NativeTypeSchema):
    TARGET_CLS = ClickHouseDateTime64WithTZNativeType  # type: ignore  # TODO: fix
    timezone_name = fields.String(required=False, load_default="UTC")
    explicit_timezone = fields.Boolean(required=False, load_default=True)


class OneOfNativeTypeSchemaBase(OneOfSchema):
    """(OneOf (Native Type) Storage Schema)"""

    type_field = "native_type_class_name"
    type_schemas = {
        schema.TARGET_CLS.native_type_class_name: schema
        for schema in (
            GenericNativeTypeSchema,
            CommonNativeTypeSchema,
            LengthedNativeTypeSchema,
            ClickHouseNativeTypeSchema,
            ClickHouseDateTimeWithTZNativeTypeSchema,
            ClickHouseDateTime64NativeTypeSchema,
            ClickHouseDateTime64WithTZNativeTypeSchema,
        )
    }

    def get_obj_type(self, obj):  # type: ignore  # TODO: fix
        return getattr(obj, self.type_field)

    def _load(self, data, *, partial=None, unknown=None):  # type: ignore  # TODO: fix
        data.setdefault(self.type_field, "common_native_type")
        return super()._load(data, partial=partial, unknown=unknown)


class OneOfNativeTypeSchema(OneOfNativeTypeSchemaBase):
    """Transition/compatibility layer. Should eventually become empty."""

    def dump(self, value, *args, **kwargs):  # type: ignore  # TODO: fix
        if value is None:
            return None

        if isinstance(value, dict):
            raise Exception("OneOfNativeTypeSchema dumping a dict")

        if isinstance(value, str):
            raise Exception("OneOfNativeTypeSchema dumping an str")

        return super().dump(value, *args, **kwargs)

    def load(self, value, *args, **kwargs):  # type: ignore  # TODO: fix
        if value is None:
            return None

        if isinstance(value, str):
            # probably a normal case until the transition is done.
            # LOGGER.info("OneOfNativeTypeSchema loading from an str")

            from bi_core.us_manager.storage_schemas.base import CtxKey

            ds_conn_type = self.context.get("ds_conn_type") or self.context[CtxKey.ds_conn_type]
            return GenericNativeType(conn_type=ds_conn_type, name=value)

        if isinstance(value, GenericNativeType):
            raise Exception("OneOfNativeTypeSchema loading an obj")

        return super().load(value, *args, **kwargs)
