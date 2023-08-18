import enum
from typing import Sequence, ClassVar, Optional

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from .dash_elements import DashElement, DashElementKind
from .filters import ComparisonOperation
from ..utils import ensure_tuple


class ValueKind(enum.Enum):
    string = enum.auto()
    multi_string = enum.auto()


@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="kind")
@attr.s(frozen=True)
class Value:
    kind = ClassVar[ValueKind]


@ModelDescriptor()
@attr.s(frozen=True)
class SingleStringValue(Value):
    kind = ValueKind.string

    value: str = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class MultiStringValue(Value):
    kind = ValueKind.multi_string

    values: Sequence[str] = attr.ib(converter=ensure_tuple)


class ValueSourceKind(enum.Enum):
    dataset_field = enum.auto()
    manual = enum.auto()


@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="kind")
@attr.s(frozen=True)
class ControlValueSource:
    kind: ClassVar[ValueSourceKind]


@ModelDescriptor()
@attr.s(frozen=True)
class ControlValueSourceDatasetField(ControlValueSource):
    kind = ValueSourceKind.dataset_field

    dataset_name: str = attr.ib()
    field_id: str = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class ControlValueSourceManual(ControlValueSource):
    kind = ValueSourceKind.manual

    parameter_name: str


@ModelDescriptor(is_abstract=True)
@attr.s(frozen=True)
class DashControl(DashElement):
    show_title: bool = attr.ib()
    title: str = attr.ib()


@ModelDescriptor(is_abstract=True)
@attr.s(frozen=True, kw_only=True)
class DashControlGuided(DashControl):
    source: ControlValueSource = attr.ib()
    default_value: Optional[Value] = attr.ib()
    comparison_operation: Optional[ComparisonOperation] = attr.ib(default=None)


@ModelDescriptor()
@attr.s(frozen=True)
class DashControlTextInput(DashControlGuided):
    kind = DashElementKind.control_text_input

    default_value: Optional[SingleStringValue] = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class DashControlSelect(DashControlGuided):
    kind = DashElementKind.control_select

    default_value: Optional[SingleStringValue] = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class DashControlMultiSelect(DashControlGuided):
    kind = DashElementKind.control_multiselect

    default_value: Optional[MultiStringValue] = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class DashControlDate(DashControlGuided):
    kind = DashElementKind.control_date

    default_value: Optional[SingleStringValue] = attr.ib()  # Later will be a date(time) value


@ModelDescriptor()
@attr.s(frozen=True)
class DashControlDateRange(DashControlGuided):
    kind = DashElementKind.control_date_range

    default_value: Optional[SingleStringValue] = attr.ib()  # Later will be a date(time) range value


@ModelDescriptor()
@attr.s(frozen=True)
class DashControlExternal(DashControl):
    kind = DashElementKind.control_external

    chart_name: str
