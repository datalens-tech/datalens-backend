from __future__ import annotations

import collections
import logging
from typing import (
    Any,
    ClassVar,
    Generic,
    Optional,
    OrderedDict,
    TypeVar,
    Union,
)

import marshmallow
from marshmallow import (
    fields,
    post_dump,
    post_load,
    pre_load,
)
from marshmallow_oneofschema import OneOfSchema

from dl_attrs_model_mapper.base import MapperBaseModel


LOGGER = logging.getLogger(__name__)

_TARGET_OBJECT_BASE_TV = TypeVar("_TARGET_OBJECT_BASE_TV")
_TARGET_OBJECT_GENERATED_TV = TypeVar("_TARGET_OBJECT_GENERATED_TV")


class BaseSchema(marshmallow.Schema, Generic[_TARGET_OBJECT_BASE_TV]):
    target_cls: ClassVar[type[_TARGET_OBJECT_BASE_TV]]  # type: ignore  # 2024-01-29 # TODO: ClassVar cannot contain type variables  [misc]
    _fields_to_skip_on_none: ClassVar[set[str]]

    # TODO FIX: Make tests
    @pre_load(pass_many=False)
    def pre_load(self, data: dict[str, Any], **_: Any) -> dict[str, Any]:
        if issubclass(self.target_cls, MapperBaseModel):
            preprocessed_data = self.target_cls.pre_load(data)
            if preprocessed_data is not None:
                return preprocessed_data

        return data

    @post_load(pass_many=False)
    def post_load(self, data: dict[str, Any], **_: Any) -> _TARGET_OBJECT_BASE_TV:
        try:
            return self.target_cls(**data)
        except Exception as exc:
            logging.exception(f"Can not instantiate class {self.target_cls}: {exc}")
            raise

    @post_dump(pass_many=False)
    def post_dump(self, data: Union[dict[str, Any], OrderedDict[str, Any]], **_: Any) -> dict[str, Any]:
        # If Meta.ordered == False MA does not respect keys order at all
        # But ordered dict will break some contracts
        # So we use that in Py>3.7 any dict is ordered to do not break contracts
        ordered_data = {k: v for k, v in data.items()} if isinstance(data, collections.OrderedDict) else data

        if len(self._fields_to_skip_on_none):
            ordered_data = {
                k: v for k, v in ordered_data.items() if k not in self._fields_to_skip_on_none or v is not None
            }

        if issubclass(self.target_cls, MapperBaseModel):
            post_processed_data = self.target_cls.post_dump(ordered_data)
            if post_processed_data is not None:
                return post_processed_data

        return ordered_data

    @classmethod
    def generate_new_regular_schema(
        cls,
        generate_for: type[_TARGET_OBJECT_GENERATED_TV],
        field_map: dict[str, fields.Field],
        fields_to_skip_on_none: Optional[set[str]] = None,
    ) -> type[BaseSchema[_TARGET_OBJECT_GENERATED_TV]]:
        # TODO FIX: Generate mnemonic class name
        class ResultingSchema(
            BaseSchema[_TARGET_OBJECT_GENERATED_TV], marshmallow.Schema.from_dict(field_map)  # type: ignore  # 2024-01-30 # TODO: Unsupported dynamic base class "marshmallow.Schema.from_dict"  [misc]
        ):
            class Meta:
                ordered = True

            target_cls = generate_for
            _fields_to_skip_on_none = fields_to_skip_on_none or set()

        return ResultingSchema


class BaseOneOfSchema(OneOfSchema):
    type_field = "type"

    type_schemas: dict[str, type[BaseSchema]] = {}  # type: ignore  # 2024-01-29 # TODO: Incompatible types in assignment (expression has type "dict[str, type[BaseSchema[Any]]]", base class "OneOfSchema" defined the type as "dict[str, type[Schema]]")  [assignment]
    _map_cls_type_discriminator: ClassVar[dict[type, str]] = {}

    @classmethod
    def register_type(cls, schema: type[BaseSchema], discriminator: str, aliases: tuple[str, ...] = tuple()) -> None:
        cls._map_cls_type_discriminator[schema.target_cls] = discriminator
        cls.type_schemas[discriminator] = schema
        for alias in aliases:
            cls.type_schemas[alias] = schema

    def get_obj_type(self, obj: Any) -> str:
        return self._map_cls_type_discriminator[type(obj)]

    def _dump(self, obj: Any, *, update_fields: bool = True, **kwargs: Any) -> dict[str, Any]:
        ret = super()._dump(obj, update_fields=update_fields, **kwargs)
        type_val = ret.pop(self.type_field)
        # Placing type field on top
        return {
            self.type_field: type_val,
            **ret,
        }

    @classmethod
    def generate_new_one_of_schema(cls, type_discriminator_field_name: str) -> type[BaseOneOfSchema]:
        # TODO FIX: Generate mnemonic class name
        class GeneratedOneOfSchema(BaseOneOfSchema):
            type_field = type_discriminator_field_name
            type_schemas: dict[str, type[BaseSchema]] = {}
            _map_cls_type_discriminator: ClassVar[dict[type, str]] = {}

        return GeneratedOneOfSchema
