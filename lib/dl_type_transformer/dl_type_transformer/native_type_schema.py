"""
Marshmallow schema for the NativeType classes.

Shared between storage (dl_core.us_manager.storage_schemas) and RQE api.
"""

from __future__ import annotations

from collections.abc import Sequence
import logging
from typing import (
    Any,
    ClassVar,
)

from marshmallow import (
    EXCLUDE,
    Schema,
    fields,
    post_load,
)
from marshmallow_oneofschema import OneOfSchema

from dl_type_transformer.native_type import (
    ClickHouseDateTime64NativeType,
    ClickHouseDateTime64WithTZNativeType,
    ClickHouseDateTimeWithTZNativeType,
    ClickHouseNativeType,
    CommonNativeType,
    GenericNativeType,
    LengthedNativeType,
)

LOGGER = logging.getLogger(__name__)


class NativeTypeSchemaBase[TARGET_TV](Schema):
    """(Shared ((Native Type) Storage Schema)), common base class for NT schemas."""

    class Meta:
        # This crutch allows the schema to ignore unknown attributes.
        # We need this because older dataset versions have `conn_type` attributes in native type objects,
        # but this attribute is no longer present in current code.
        # To avoid this conflict we need to ignore extra attributes.
        # TODO: Eventually datasets should be migrated so that this can be removed
        unknown = EXCLUDE

    TARGET_CLS: ClassVar[type[TARGET_TV]]

    @post_load(pass_many=False)
    def to_object(self, data: dict, **_: Any) -> TARGET_TV:
        if "conn_type" in data:
            # TODO: Remove once all datasets have been migrated
            data = data.copy()
            data.pop("conn_type")

        return self.TARGET_CLS(**data)


class GenericNativeTypeSchema(NativeTypeSchemaBase[GenericNativeType]):
    """
    ((Generic (Native Type)) Storage Schema),
    (not to be confused with (Generic ((Native Type) Storage Schema))).
    """

    TARGET_CLS = GenericNativeType

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
    TARGET_CLS = ClickHouseDateTime64WithTZNativeType
    timezone_name = fields.String(required=False, load_default="UTC")
    explicit_timezone = fields.Boolean(required=False, load_default=True)


class OneOfNativeTypeSchemaBase(OneOfSchema):
    """(OneOf (Native Type) Storage Schema)"""

    class Meta:
        # Same as above in `NativeTypeSchemaBase`, to ignore `conn_type`
        unknown = EXCLUDE

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

    def dump(self, obj: Any, *, many: bool | None = None, **kwargs: Any) -> Any:
        if obj is None:
            return None

        if isinstance(obj, dict):
            raise Exception("OneOfNativeTypeSchema dumping a dict")

        if isinstance(obj, str):
            raise Exception("OneOfNativeTypeSchema dumping an str")

        return super().dump(obj, many=many, **kwargs)

    def load(
        self,
        data: Any,
        *,
        many: bool | None = None,
        partial: bool | Sequence[str] | set[str] | None = None,
        unknown: str | None = None,
        **kwargs: Any,
    ) -> Any:
        if data is None:
            return None

        if isinstance(data, str):
            # probably a normal case until the transition is done.
            # LOGGER.info("OneOfNativeTypeSchema loading from an str")

            return GenericNativeType(name=data)

        if isinstance(data, GenericNativeType):
            raise Exception("OneOfNativeTypeSchema loading an obj")

        return super().load(data, many=many, partial=partial, unknown=unknown, **kwargs)
