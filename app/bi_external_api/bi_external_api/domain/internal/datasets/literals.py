from typing import ClassVar

import attr

from bi_constants.enums import BIType
from bi_external_api.attrs_model_mapper import ModelDescriptor


@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="type")
@attr.s(frozen=True)
class DefaultValue:
    type: ClassVar[BIType]


@ModelDescriptor()
@attr.s()
class DefaultValueString(DefaultValue):
    type = BIType.string

    value: str = attr.ib()


@ModelDescriptor()
@attr.s()
class DefaultValueInteger(DefaultValue):
    type = BIType.integer

    value: int = attr.ib()


@ModelDescriptor()
@attr.s()
class DefaultValueFloat(DefaultValue):
    type = BIType.float

    value: float = attr.ib()

# TODO FIX: BI-3712 Support other types of literals
