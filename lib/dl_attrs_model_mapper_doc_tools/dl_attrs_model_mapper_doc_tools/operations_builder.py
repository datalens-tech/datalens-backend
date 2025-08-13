import enum
from typing import (
    Any,
    Generic,
    Optional,
    Sequence,
    TypeVar,
)

import attr

from dl_attrs_model_mapper.base import ModelDescriptor
from dl_attrs_model_mapper.domain import AmmRegularSchema
from dl_attrs_model_mapper.marshmallow import ModelMapperMarshmallow
from dl_attrs_model_mapper.marshmallow_base_schemas import BaseOneOfSchema
from dl_attrs_model_mapper.utils import MText
from dl_attrs_model_mapper_doc_tools.domain import (
    AmmOperation,
    AmmOperationExample,
)


_OPERATION_KIND_ENUM_TV = TypeVar("_OPERATION_KIND_ENUM_TV", bound=enum.Enum)


@attr.s(auto_attribs=True, kw_only=True)
class OperationExample:
    title: Optional[MText] = None
    description: Optional[MText] = None
    rq: Any
    rs: Any


@attr.s()
class UserOperationInfo(Generic[_OPERATION_KIND_ENUM_TV]):
    kind: _OPERATION_KIND_ENUM_TV = attr.ib()
    description: MText = attr.ib()
    example_list: list[OperationExample] = attr.ib(factory=list)

    @classmethod
    def validate_example_kind(cls, data_name: str, expected_kind: Any, data: Any) -> None:
        kind = getattr(data, "kind", None)

        if kind != expected_kind:
            raise AssertionError(f"Got unexpected kind for example {data_name}: {kind=}")

    def __attrs_post_init__(self) -> None:
        for idx, example in enumerate(self.example_list):
            self.validate_example_kind(f"{self.kind.name}/{idx=}/RQ", expected_kind=self.kind, data=example.rq)
            self.validate_example_kind(f"{self.kind.name}/{idx=}/RS", expected_kind=self.kind, data=example.rs)


@attr.s()
class AmmOperationsBuilder(Generic[_OPERATION_KIND_ENUM_TV]):
    operation_kind_enum: type[_OPERATION_KIND_ENUM_TV] = attr.ib()
    user_op_info_list: Sequence[UserOperationInfo[_OPERATION_KIND_ENUM_TV]] = attr.ib()
    rq_base_type: type = attr.ib()
    rs_base_type: type = attr.ib()
    model_mapper: ModelMapperMarshmallow = attr.ib()

    def _resolve_regular_schema(self, generic_model_type: type, discriminator_value: str) -> AmmRegularSchema:
        schema_registry = self.model_mapper.get_amm_schema_registry()
        return schema_registry.get_generic_type_schema(generic_model_type).mapping[discriminator_value]

    def _get_generic_schema_for_type(self, t: type) -> BaseOneOfSchema:
        schema_cls = self.model_mapper.get_schema_for_attrs_class(t)
        assert issubclass(schema_cls, BaseOneOfSchema)
        return schema_cls()

    def _create_amm_operation_for_kind(
        self,
        user_op_info: UserOperationInfo[_OPERATION_KIND_ENUM_TV],
        rq_base_model_descriptor: ModelDescriptor,
    ) -> AmmOperation:
        discriminator_name = rq_base_model_descriptor.children_type_discriminator_attr_name
        assert discriminator_name is not None
        discriminator_value = user_op_info.kind.name

        amm_examples = [
            AmmOperationExample(
                title=user_example.title,
                description=user_example.description,
                rq=self._get_generic_schema_for_type(self.rq_base_type).dump(user_example.rq),
                rs=self._get_generic_schema_for_type(self.rs_base_type).dump(user_example.rs),
            )
            for user_example in user_op_info.example_list
        ]

        return AmmOperation(
            amm_schema_rq=self._resolve_regular_schema(self.rq_base_type, discriminator_value),
            amm_schema_rs=self._resolve_regular_schema(self.rs_base_type, discriminator_value),
            code=discriminator_value,
            discriminator_attr_name=discriminator_name,
            description=user_op_info.description,
            examples=amm_examples,
        )

    def build(self) -> Sequence[AmmOperation]:
        rq_base_model_descriptor = ModelDescriptor.get_for_type(self.rq_base_type)
        assert (
            rq_base_model_descriptor is not None
        ), f"No model descriptor defined for base request class: {self.rq_base_type}"

        return [
            self._create_amm_operation_for_kind(
                user_op_info,
                rq_base_model_descriptor,
            )
            for user_op_info in self.user_op_info_list
        ]
