from typing import ClassVar

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from dl_constants.enums import UserDataType


@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="type")
@attr.s(frozen=True)
class DefaultValue:
    type: ClassVar[UserDataType]


@ModelDescriptor()
@attr.s()
class DefaultValueString(DefaultValue):
    type = UserDataType.string

    value: str = attr.ib()


@ModelDescriptor()
@attr.s()
class DefaultValueInteger(DefaultValue):
    type = UserDataType.integer

    value: int = attr.ib()


@ModelDescriptor()
@attr.s()
class DefaultValueFloat(DefaultValue):
    type = UserDataType.float

    value: float = attr.ib()


# TODO FIX: BI-3712 Support other types of literals
