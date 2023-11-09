from __future__ import annotations

import enum
import json
from typing import (
    Any,
    Iterable,
    Optional,
    Type,
)

import attr
from dynamic_enum import DynamicEnum
import marshmallow
from marshmallow import fields

from dl_attrs_model_mapper.base import (
    AttribDescriptor,
    ModelDescriptor,
)
from dl_attrs_model_mapper.domain import (
    AmmEnumField,
    AmmField,
    AmmGenericSchema,
    AmmListField,
    AmmNestedField,
    AmmRegularSchema,
    AmmScalarField,
    AmmSchema,
    AmmSchemaRegistry,
    AmmStringMappingField,
)
from dl_attrs_model_mapper.marshmallow_base_schemas import (
    BaseOneOfSchema,
    BaseSchema,
)
from dl_attrs_model_mapper.marshmallow_fields import (
    FrozenMappingStrToStrOrStrSeqField,
    FrozenStrMappingField,
    SingleOrMultiStringField,
)
from dl_attrs_model_mapper.structs.mappings import FrozenMappingStrToStrOrStrSeq
from dl_attrs_model_mapper.structs.singleormultistring import SingleOrMultiString
from dl_attrs_model_mapper.utils import (
    CommonAttributeProps,
    is_sequence,
    is_str_mapping,
    unwrap_typing_container_with_single_type,
)
from dl_model_tools.schema.dynamic_enum_field import DynamicEnumField


@attr.s(frozen=True, auto_attribs=True)
class FieldBundle:
    ma_field: fields.Field
    amm_field: AmmField


@attr.s(frozen=True, auto_attribs=True)
class SchemaBundle:
    schema_cls: Type[marshmallow.Schema]
    amm_schema: AmmSchema


@attr.s(frozen=True, auto_attribs=True)
class RegularSchemaBundle(SchemaBundle):
    schema_cls: Type[BaseSchema]
    amm_schema: AmmRegularSchema


@attr.s(frozen=True, auto_attribs=True)
class GenericSchemaBundle(SchemaBundle):
    schema_cls: Type[BaseOneOfSchema]
    amm_schema: AmmGenericSchema


@attr.s
class ModelMapperMarshmallow:
    _map_complex_type_schema_bundle: dict[type, SchemaBundle] = attr.ib(factory=dict)

    _map_scalar_type_field_cls: dict[type, Type[marshmallow.fields.Field]] = attr.ib(
        factory=lambda: {
            int: fields.Integer,
            str: fields.String,
            float: fields.Float,
            bool: fields.Boolean,
            FrozenMappingStrToStrOrStrSeq: FrozenMappingStrToStrOrStrSeqField,
            SingleOrMultiString: SingleOrMultiStringField,
        }
    )

    _map_scalar_type_schema_cls: dict[type, Type[marshmallow.Schema]] = attr.ib(factory=dict)

    def handle_single_attr_ib(self, attr_ib: attr.Attribute) -> FieldBundle:
        attrib_descriptor = AttribDescriptor.from_attrib(attr_ib)
        return self.create_field_for_type(
            the_type=attr_ib.type,
            attrib_descriptor=attrib_descriptor,
            ma_attribute_name=attr_ib.name,
            is_required=attr_ib.default is attr.NOTHING,
        )

    def create_field_for_type(
        self,
        the_type: Any,
        attrib_descriptor: Optional[AttribDescriptor],
        ma_attribute_name: Optional[str],
        is_required: bool,
        is_optional: bool = False,
    ) -> FieldBundle:
        container_type, effective_type = unwrap_typing_container_with_single_type(the_type)

        if container_type is Optional:
            assert is_optional is False
            return self.create_field_for_type(
                the_type=effective_type,
                attrib_descriptor=attrib_descriptor,
                ma_attribute_name=ma_attribute_name,
                is_required=is_required,
                is_optional=True,
            )

        common_props = CommonAttributeProps(
            allow_none=is_optional,
            attribute_name=ma_attribute_name,
            required=is_required,
            load_only=attrib_descriptor.load_only if attrib_descriptor is not None else False,
            description=attrib_descriptor.description if attrib_descriptor is not None else None,
        )

        if container_type is None:
            return self.create_field_for_unwrapped_type(effective_type, attrib_descriptor, common_props)
        elif is_sequence(container_type):
            nested_field_bundle = self.create_field_for_type(
                effective_type,
                attrib_descriptor,
                ma_attribute_name=None,
                is_required=False,
            )
            return FieldBundle(
                ma_field=fields.List(nested_field_bundle.ma_field, **common_props.to_common_ma_field_kwargs()),
                amm_field=AmmListField(common_props, nested_field_bundle.amm_field),
            )
        elif is_str_mapping(container_type):
            nested_field_bundle = self.create_field_for_type(
                effective_type,
                attrib_descriptor,
                ma_attribute_name=None,
                is_required=False,
            )
            return FieldBundle(
                ma_field=FrozenStrMappingField(
                    keys=fields.String(),
                    values=nested_field_bundle.ma_field,
                    **common_props.to_common_ma_field_kwargs(),
                ),
                amm_field=AmmStringMappingField(common_props, nested_field_bundle.amm_field),
            )

        else:
            raise AssertionError(
                f"Got unexpected container type from unwrap_typing_container_with_single_type(): {container_type!r}"
            )

    def create_field_for_unwrapped_type(
        self,
        the_type: Type,
        attrib_descriptor: Optional[AttribDescriptor],
        common_ma_field_kwargs: CommonAttributeProps,
    ) -> FieldBundle:
        if attr.has(the_type):
            schema_bundle = self.get_or_create_schema_bundle_for_attrs_class(the_type)
            return FieldBundle(
                ma_field=fields.Nested(schema_bundle.schema_cls, **common_ma_field_kwargs.to_common_ma_field_kwargs()),
                amm_field=AmmNestedField(common_ma_field_kwargs, schema_bundle.amm_schema),
            )

        elif the_type in self._map_scalar_type_schema_cls:
            return FieldBundle(
                ma_field=fields.Nested(
                    self._map_scalar_type_schema_cls[the_type],
                    **common_ma_field_kwargs.to_common_ma_field_kwargs(),
                ),
                amm_field=AmmScalarField(common_ma_field_kwargs, the_type),
            )

        elif the_type in self._map_scalar_type_field_cls:
            return FieldBundle(
                ma_field=self._map_scalar_type_field_cls[the_type](
                    **common_ma_field_kwargs.to_common_ma_field_kwargs(),
                ),
                amm_field=AmmScalarField(common_ma_field_kwargs, the_type),
            )

        elif isinstance(the_type, type) and issubclass(the_type, enum.Enum):
            enum_by_value = False if attrib_descriptor is None else attrib_descriptor.enum_by_value

            return FieldBundle(
                ma_field=fields.Enum(
                    the_type,
                    by_value=enum_by_value,
                    **common_ma_field_kwargs.to_common_ma_field_kwargs(),
                ),
                amm_field=AmmEnumField(
                    common_ma_field_kwargs,
                    str,
                    values=[str(m.value) if enum_by_value else m.name for m in the_type],
                ),
            )

        elif isinstance(the_type, type) and issubclass(the_type, DynamicEnum):
            return FieldBundle(
                ma_field=DynamicEnumField(
                    the_type,
                    **common_ma_field_kwargs.to_common_ma_field_kwargs(),
                ),
                amm_field=AmmEnumField(
                    common_ma_field_kwargs,
                    str,
                    values=[m.value for m in the_type],
                ),
            )

        else:
            raise TypeError(f"Can not build field for {the_type!r}")

    def link_to_parents(self, the_type: Type, the_schema_bundle: RegularSchemaBundle) -> None:
        the_type_model_descriptor = ModelDescriptor.get_for_type(the_type)
        assert not the_type_model_descriptor.is_abstract

        registered_parents_types_with_schema_bundle = [
            (candidate_type, candidate_schema_bundle)
            for candidate_type, candidate_schema_bundle in self._map_complex_type_schema_bundle.items()
            if issubclass(the_type, candidate_type) and ModelDescriptor.get_for_type(candidate_type).is_abstract
        ]

        for _, parent_schema_bundle in registered_parents_types_with_schema_bundle:
            assert isinstance(parent_schema_bundle, GenericSchemaBundle)

            parent_schema_bundle.schema_cls.register_type(
                the_schema_bundle.schema_cls,
                the_type_model_descriptor.effective_type_discriminator,
                the_type_model_descriptor.effective_type_discriminator_aliases,
            )
            parent_schema_bundle.amm_schema.register_sub_schema(
                the_schema_bundle.amm_schema, the_type_model_descriptor.effective_type_discriminator
            )

    def link_to_children(self, the_type: Type, generic_bundle: GenericSchemaBundle) -> None:
        the_type_model_descriptor = ModelDescriptor.get_for_type(the_type)
        assert the_type_model_descriptor.is_abstract

        registered_children_types_with_schema_bundle = [
            (candidate_type, candidate_schema_bundle)
            for candidate_type, candidate_schema_bundle in self._map_complex_type_schema_bundle.items()
            if issubclass(candidate_type, the_type) and not ModelDescriptor.get_for_type(candidate_type).is_abstract
        ]

        for child_type, child_schema_bundle in registered_children_types_with_schema_bundle:
            assert isinstance(child_schema_bundle, RegularSchemaBundle)

            child_type_discriminator = ModelDescriptor.get_for_type(child_type).effective_type_discriminator
            discriminator_aliases = ModelDescriptor.get_for_type(child_type).effective_type_discriminator_aliases

            generic_bundle.schema_cls.register_type(
                child_schema_bundle.schema_cls,
                child_type_discriminator,
                discriminator_aliases,
            )
            generic_bundle.amm_schema.register_sub_schema(
                child_schema_bundle.amm_schema,
                child_type_discriminator,
            )

    @staticmethod
    def resolve_ma_field_name(attr_ib: attr.Attribute) -> str:
        attrib_descriptor = AttribDescriptor.from_attrib(attr_ib)

        if attrib_descriptor is not None:
            field_name_override = attrib_descriptor.serialization_key
            if field_name_override is not None:
                return field_name_override

        return attr_ib.name

    def get_schema_for_attrs_class(self, target_type: Type) -> Type[marshmallow.Schema]:
        if target_type in self._map_complex_type_schema_bundle:
            return self._map_complex_type_schema_bundle[target_type].schema_cls
        raise AssertionError(f"Schema for {type} was not created")

    def get_or_create_schema_for_attrs_class(self, target: Type) -> Type[marshmallow.Schema]:
        return self.get_or_create_schema_bundle_for_attrs_class(target).schema_cls

    def get_or_create_schema_bundle_for_attrs_class(self, target: Type) -> SchemaBundle:
        assert attr.has(target), f"Schema creation requested for non-attrs class: {target}"
        target_model_descriptor = ModelDescriptor.get_for_type(target)

        if target in self._map_complex_type_schema_bundle:
            return self._map_complex_type_schema_bundle[target]

        # TODO FIX: Determine why MyPy thinks "error: Module has no attribute "resolve_types"
        attr.resolve_types(target)  # type: ignore
        schema_bundle: SchemaBundle

        if target_model_descriptor.is_abstract:
            type_discriminator_field_name: str = target_model_descriptor.children_type_discriminator_attr_name or "type"

            abstract_schema_bundle = GenericSchemaBundle(
                schema_cls=BaseOneOfSchema.generate_new_one_of_schema(type_discriminator_field_name),
                amm_schema=AmmGenericSchema(clz=target, discriminator_property_name=type_discriminator_field_name),
            )
            self.link_to_children(target, abstract_schema_bundle)

            schema_bundle = abstract_schema_bundle
        else:
            fields_bundle_map: dict[str, FieldBundle] = {}
            fields_to_skip_on_none: list[str] = []
            for attr_ib in attr.fields(target):
                fields_bundle_map[self.resolve_ma_field_name(attr_ib)] = self.handle_single_attr_ib(attr_ib)
                attr_descriptor = AttribDescriptor.from_attrib(attr_ib)
                if attr_descriptor and attr_descriptor.skip_none_on_dump:
                    fields_to_skip_on_none.append(attr_ib.name)

            regular_bundle = RegularSchemaBundle(
                schema_cls=BaseSchema.generate_new_regular_schema(
                    generate_for=target,
                    field_map={f_name: f_bundle.ma_field for f_name, f_bundle in fields_bundle_map.items()},
                    fields_to_skip_on_none=set(fields_to_skip_on_none),
                ),
                amm_schema=AmmRegularSchema(
                    clz=target,
                    fields={f_name: f_bundle.amm_field for f_name, f_bundle in fields_bundle_map.items()},
                    description=target_model_descriptor.description,
                ),
            )
            self.link_to_parents(target, regular_bundle)

            schema_bundle = regular_bundle

        self._map_complex_type_schema_bundle[target] = schema_bundle
        return schema_bundle

    def register_models(self, models: Iterable[Any]) -> None:
        for model in models:
            self.get_or_create_schema_for_attrs_class(model)

    def get_amm_schema_registry(self) -> AmmSchemaRegistry:
        return AmmSchemaRegistry.from_schemas_collection(
            [bundle.amm_schema for bundle in self._map_complex_type_schema_bundle.values()]
        )

    def dump_external_model_to_str(self, model: Any) -> str:
        try:
            schema = self.get_or_create_schema_for_attrs_class(type(model))()
            return schema.dumps(model)
        except Exception:  # noqa
            return json.dumps({"kind": "serialization_error"})
