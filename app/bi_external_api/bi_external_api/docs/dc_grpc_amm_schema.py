from typing import (
    Any,
    Optional,
    Type,
)

import attr

from bi_external_api.attrs_model_mapper.domain import (
    AmmEnumDescriptor,
    AmmEnumField,
    AmmEnumMemberDescriptor,
    AmmField,
    AmmListField,
    AmmNestedField,
    AmmOneOfDescriptorField,
    AmmRegularSchema,
    AmmScalarField,
    AmmSchema,
    AmmSchemaRegistry,
)
from bi_external_api.attrs_model_mapper.utils import (
    CommonAttributeProps,
    MText,
)
from bi_external_api.attrs_model_mapper_docs.domain import AmmOperation


@attr.s(auto_attribs=True, frozen=True, kw_only=True)
class ExternalClassDef:
    full_name: str


class BaseScalar:
    pass


class BaseEnum(BaseScalar):
    pass


class BaseRegularSchema:
    pass


@attr.s()
class ProtoGenDocAccessor:
    d: dict[str, Any] = attr.ib()
    external_class_defs: set[ExternalClassDef] = attr.ib()

    _map_msg_full_name_dict: dict[str, dict[str, Any]] = attr.ib(init=False, factory=dict)
    _map_svc_full_name_dict: dict[str, dict[str, Any]] = attr.ib(init=False, factory=dict)
    _map_enum_full_name_dict: dict[str, dict[str, Any]] = attr.ib(init=False, factory=dict)

    _map_msg_full_name_pseudo_type: dict[str, Type] = attr.ib(init=False, factory=dict)

    _amm_schema_registry: AmmSchemaRegistry = attr.ib(init=False, factory=AmmSchemaRegistry)

    @staticmethod
    def _map_full_named_dicts(collection: list[dict[str, Any]], mapping: dict[str, Any]) -> None:
        for item in collection:
            full_name = item["fullName"]

            if full_name in mapping:
                raise ValueError(f"Duplicated item full name: {full_name}")

            mapping[full_name] = item

    def __attrs_post_init__(self) -> None:
        for file_dict in self.d["files"]:
            self._map_full_named_dicts(file_dict["messages"], self._map_msg_full_name_dict)
            self._map_full_named_dicts(file_dict["services"], self._map_svc_full_name_dict)
            self._map_full_named_dicts(file_dict["enums"], self._map_enum_full_name_dict)

        for message_full_name in self._map_msg_full_name_dict:
            self.get_schema_for_message(message_full_name)

    def get_regular_type(self, full_name: str) -> Type:
        if full_name in self._map_msg_full_name_pseudo_type:
            return self._map_msg_full_name_pseudo_type[full_name]

        ret: Type

        cls_name = full_name.replace(".", "_dot_")

        if full_name in {etd.full_name for etd in self.external_class_defs}:
            ret = attr.make_class(cls_name, [], (BaseScalar,))
        elif full_name in self._map_enum_full_name_dict:
            ret = attr.make_class(cls_name, [], (BaseEnum,))
        else:
            ret = attr.make_class(cls_name, [], (BaseRegularSchema,))

        self._map_msg_full_name_pseudo_type[full_name] = ret
        return ret

    def create_common_props(self, field_dict: dict[str, Any]) -> CommonAttributeProps:
        description = field_dict["description"]
        return CommonAttributeProps(
            attribute_name=field_dict["name"],
            description=MText(en=description, ru=description) if description else None,
            required=False,
            allow_none=True,
            load_only=False,
        )

    def create_amm_field(
        self,
        field_dict: dict[str, Any],
        common_props_override: Optional[CommonAttributeProps] = None,
    ) -> AmmField:
        full_type_name = field_dict["fullType"]
        pseudo_type = self.get_regular_type(full_type_name)
        common_props = self.create_common_props(field_dict) if common_props_override is None else common_props_override

        label = field_dict["label"]

        if label == "repeated":
            return AmmListField(
                common_props,
                self.create_amm_field(
                    {
                        **field_dict,
                        "label": "",
                    },
                    common_props_override=attr.evolve(common_props, description=None),
                ),
            )
        elif label == "":
            pass
        else:
            raise ValueError(f"Got unexpected label {label!r} for field {field_dict!r}")

        if issubclass(pseudo_type, BaseScalar):
            if issubclass(pseudo_type, BaseEnum):
                enum_dict = self._map_enum_full_name_dict[full_type_name]
                return AmmEnumField(
                    common_props,
                    pseudo_type,
                    values=[val["name"] for val in enum_dict["values"]],
                    scalar_type_identifier=full_type_name,
                )
            else:
                return AmmScalarField(common_props, pseudo_type, scalar_type_identifier=full_type_name)
        elif issubclass(pseudo_type, BaseRegularSchema):
            return AmmNestedField(common_props, self.get_schema_for_message(full_type_name))
        else:
            raise AssertionError(f"get_regular_type() returns unexpected pseudo class for {full_type_name}")

    def get_schema_for_message(self, full_name: str) -> AmmSchema:
        pseudo_clz = self.get_regular_type(full_name)
        if not issubclass(pseudo_clz, BaseRegularSchema):
            raise ValueError(f"Can not create schema for {full_name} due to it was resolved not as internal PB message")

        if self._amm_schema_registry.is_registered(pseudo_clz):
            return self._amm_schema_registry.get_regular_type_schema(pseudo_clz)

        msg_dict = self._map_msg_full_name_dict[full_name]

        schema_fields: dict[str, AmmField] = {}

        for field_dict in msg_dict["fields"]:
            field_name = field_dict["name"]

            if field_dict["isoneof"]:
                one_of_field_name = field_dict["oneofdecl"]
                one_of_field: AmmOneOfDescriptorField

                if one_of_field_name in schema_fields:
                    may_be_one_of_field = schema_fields[one_of_field_name]
                    assert isinstance(may_be_one_of_field, AmmOneOfDescriptorField)
                    one_of_field = may_be_one_of_field

                else:
                    one_of_field = AmmOneOfDescriptorField(self.create_common_props(field_dict), [])
                    schema_fields[one_of_field_name] = one_of_field

                one_of_field.field_names.append(field_name)

            schema_fields[field_name] = self.create_amm_field(field_dict)

        schema = AmmRegularSchema(
            clz=pseudo_clz,
            fields=schema_fields,
            identifier=full_name,
        )
        self._amm_schema_registry.register(schema)
        return schema

    def get_enum(self, full_name: str) -> AmmEnumDescriptor:
        enum_dict = self._map_enum_full_name_dict[full_name]
        enum_description: str = enum_dict["description"]

        return AmmEnumDescriptor(
            type_identifier=full_name,
            description=MText(en=enum_description, ru=enum_description) if enum_description else None,
            members=[
                AmmEnumMemberDescriptor(
                    key=member["name"],
                    description=MText(
                        en=member["description"],
                        ru=member["description"],
                    )
                    if member["description"]
                    else None,
                )
                for member in enum_dict["values"]
            ],
        )

    def get_operations(self, svc_name: str) -> list[AmmOperation]:
        svc_dict = self._map_svc_full_name_dict[svc_name]

        return [
            AmmOperation(
                description=MText(en=method["description"], ru=method["description"]),
                # Due to it should go to title
                code=method["name"],
                discriminator_attr_name="",
                amm_schema_rq=self._amm_schema_registry.get_regular_type_schema(
                    self.get_regular_type(method["requestFullType"])
                ),
                amm_schema_rs=self._amm_schema_registry.get_regular_type_schema(
                    self.get_regular_type(method["responseFullType"])
                ),
                examples=[],
            )
            for method in svc_dict["methods"]
        ]

    def get_service_description(self, svc_id: str) -> Optional[str]:
        return self._map_svc_full_name_dict[svc_id]["description"] or None

    def get_services_by_prefix(self, prefix: str) -> list[str]:
        return [svc_name for svc_name in self._map_svc_full_name_dict if svc_name.startswith(prefix)]

    def get_messages_by_prefix(self, prefix: str) -> list[str]:
        return [msg_name for msg_name in self._map_msg_full_name_dict if msg_name.startswith(prefix)]

    def get_enums_by_prefix(self, prefix: str) -> list[str]:
        return [enum_name for enum_name in self._map_enum_full_name_dict if enum_name.startswith(prefix)]
