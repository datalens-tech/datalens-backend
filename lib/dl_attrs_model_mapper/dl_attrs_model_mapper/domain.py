import abc
from typing import (
    Any,
    ClassVar,
    Collection,
    Optional,
    Sequence,
    Type,
)

import attr

from dl_attrs_model_mapper.utils import (
    CommonAttributeProps,
    MText,
)


@attr.s()
class AmmSchemaRegistry:
    _map_type_schema: dict[Type, "AmmSchema"] = attr.ib(factory=dict)
    _map_type_name: dict[Type, str] = attr.ib(factory=dict)

    def register(self, schema: "AmmSchema") -> None:
        self._map_type_schema[schema.clz] = schema
        self._map_type_name[schema.clz] = schema.clz.__name__

    def is_registered(self, clz: Type) -> bool:
        return clz in self._map_type_schema

    def get_ref_for_type(self, clz: Type) -> str:
        return f"#/components/schemas/{self._map_type_name[clz]}"

    def get_generic_type_schema(self, clz: Type) -> "AmmGenericSchema":
        ret = self._map_type_schema[clz]
        assert isinstance(ret, AmmGenericSchema)
        return ret

    def get_regular_type_schema(self, clz: Type) -> "AmmRegularSchema":
        ret = self._map_type_schema[clz]
        assert isinstance(ret, AmmRegularSchema)
        return ret

    @classmethod
    def from_schemas_collection(cls, schema_collection: Collection["AmmSchema"]) -> "AmmSchemaRegistry":
        reg = cls()
        for schema in schema_collection:
            reg.register(schema)
        return reg

    def dump_open_api_schemas(self) -> dict[str, dict[str, Any]]:
        return {
            self._map_type_name[schema.clz]: schema.to_openapi_dict(self) for schema in self._map_type_schema.values()
        }


@attr.s()
class AmmField:
    common_props: CommonAttributeProps = attr.ib()

    def to_openapi_dict(self, ref_resolver: AmmSchemaRegistry, *, is_root_prop: bool) -> dict[str, Any]:
        ret: dict[str, Any] = dict(nullable=self.common_props.allow_none)
        if self.common_props.load_only:
            ret["writeOnly"] = True
        return ret


@attr.s()
class AmmScalarField(AmmField):
    scalar_type: Type = attr.ib()
    scalar_type_identifier: Optional[str] = attr.ib(default=None)

    TYPE_MAP: ClassVar[dict[Type, str]] = {
        int: "number",
        str: "string",
        float: "number",
        bool: "boolean",
    }

    def to_openapi_dict(self, ref_resolver: AmmSchemaRegistry, *, is_root_prop: bool) -> dict[str, Any]:
        return {
            "type": self.TYPE_MAP[self.scalar_type],
            **super().to_openapi_dict(ref_resolver, is_root_prop=is_root_prop),
        }


@attr.s()
class AmmEnumField(AmmScalarField):
    values: Sequence[Any] = attr.ib(kw_only=True)

    def to_openapi_dict(self, ref_resolver: AmmSchemaRegistry, *, is_root_prop: bool) -> dict[str, Any]:
        return dict(
            **super().to_openapi_dict(ref_resolver, is_root_prop=is_root_prop),
            enum=list(sorted(self.values)),
        )


@attr.s()
class AmmNestedField(AmmField):
    item: "AmmSchema" = attr.ib()

    def to_openapi_dict(self, ref_resolver: AmmSchemaRegistry, *, is_root_prop: bool) -> dict[str, Any]:
        return {
            **super().to_openapi_dict(ref_resolver, is_root_prop=is_root_prop),
            "allOf": [{"$ref": ref_resolver.get_ref_for_type(self.item.clz)}],
        }


@attr.s()
class AmmListField(AmmField):
    item: "AmmField" = attr.ib()

    def to_openapi_dict(self, ref_resolver: AmmSchemaRegistry, *, is_root_prop: bool) -> dict[str, Any]:
        return dict(type="array", items=self.item.to_openapi_dict(ref_resolver, is_root_prop=False))


@attr.s()
class AmmStringMappingField(AmmField):
    value: "AmmField" = attr.ib()

    def to_openapi_dict(self, ref_resolver: AmmSchemaRegistry, *, is_root_prop: bool) -> dict[str, Any]:
        return dict(type="object", additionalProperties=self.value.to_openapi_dict(ref_resolver, is_root_prop=False))


@attr.s()
class AmmOneOfDescriptorField(AmmField):
    """
    Workaround for GRPC oneof's with external/scalars that do not suit to OpenAPI like one-of/inheritance.
    Assumed that will be used only for generating docs by protospecs.
    """

    field_names: list[str] = attr.ib()


@attr.s(kw_only=True)
class AmmEnumMemberDescriptor:
    key: str = attr.ib()
    description: Optional[MText] = attr.ib(default=None)


@attr.s(kw_only=True)
class AmmEnumDescriptor:
    type_identifier: str = attr.ib()
    description: Optional[MText] = attr.ib(default=None)
    members: list[AmmEnumMemberDescriptor] = attr.ib()


#
# Schemas
#
@attr.s(kw_only=True)
class AmmSchema(metaclass=abc.ABCMeta):
    clz: Type = attr.ib()
    identifier: Optional[str] = attr.ib(default=None)

    @abc.abstractmethod
    def to_openapi_dict(self, ref_resolver: AmmSchemaRegistry) -> dict[str, Any]:
        raise NotImplementedError()


@attr.s()
class AmmRegularSchema(AmmSchema):
    fields: dict[str, AmmField] = attr.ib()
    description: Optional[MText] = attr.ib(default=None)

    def to_openapi_dict(self, ref_resolver: AmmSchemaRegistry) -> dict[str, Any]:
        ret = {
            "type": "object",
            "properties": {
                f_name: field.to_openapi_dict(ref_resolver, is_root_prop=True) for f_name, field in self.fields.items()
            },
        }
        required_names = [f_name for f_name, field in self.fields.items() if field.common_props.required]
        if required_names:
            ret.update(required=required_names)
        return ret


@attr.s()
class AmmGenericSchema(AmmSchema):
    discriminator_property_name: str = attr.ib()
    mapping: dict[str, AmmRegularSchema] = attr.ib(factory=dict)

    def register_sub_schema(self, schema: AmmRegularSchema, discriminator: str) -> None:
        assert discriminator not in self.mapping, f"discriminator f{discriminator} already registered"
        self.mapping[discriminator] = schema

    def to_openapi_dict(self, ref_resolver: AmmSchemaRegistry) -> dict[str, Any]:
        return {
            "oneOf": [{"$ref": ref_resolver.get_ref_for_type(schema.clz)} for schema in self.mapping.values()],
            "discriminator": {
                "propertyName": self.discriminator_property_name,
                "mapping": {discr: ref_resolver.get_ref_for_type(schema.clz) for discr, schema in self.mapping.items()},
            },
        }
