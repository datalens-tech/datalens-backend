import enum
from typing import (
    ClassVar,
    Optional,
    Sequence,
)

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from bi_external_api.domain.utils import ensure_tuple

from .chart_fields import ChartFieldSource


class ColoringKind(enum.Enum):
    dimension = "dimension"
    measure = "measure"


class MeasureColoringSpecKind(enum.Enum):
    gradient_2_points = "gradient_2_points"
    gradient_3_points = "gradient_3_points"


@ModelDescriptor()
@attr.s(frozen=True, auto_attribs=True)
class ColorMount:
    value: str
    color_idx: int


@ModelDescriptor()
@attr.s(frozen=True, auto_attribs=True)
class Thresholds2:
    left: float = attr.ib()
    right: float = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True, auto_attribs=True)
class Thresholds3:
    left: float = attr.ib()
    middle: float = attr.ib()
    right: float = attr.ib()


@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="kind")
@attr.s(frozen=True, auto_attribs=True)
class MeasureColoringSpec:
    kind: ClassVar[MeasureColoringSpecKind]
    # Not in MeasureColoring to validate gradients later on model
    palette: Optional[str] = attr.ib(default=None)


@ModelDescriptor()
@attr.s(frozen=True, auto_attribs=True)
class Gradient2(MeasureColoringSpec):
    kind = MeasureColoringSpecKind.gradient_2_points

    thresholds: Optional[Thresholds2] = attr.ib(default=None)


@ModelDescriptor()
@attr.s(frozen=True, auto_attribs=True)
class Gradient3(MeasureColoringSpec):
    kind = MeasureColoringSpecKind.gradient_3_points

    thresholds: Optional[Thresholds3] = attr.ib(default=None)


@ModelDescriptor(is_abstract=True, children_type_discriminator_attr_name="kind")
@attr.s(frozen=True)
class FieldColoring:
    kind: ClassVar[ColoringKind]

    source: ChartFieldSource = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class MeasureColoring(FieldColoring):
    kind = ColoringKind.measure

    spec: MeasureColoringSpec = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class DimensionColoring(FieldColoring):
    kind = ColoringKind.dimension

    palette_id: Optional[str] = attr.ib()
    mounts: Sequence[ColorMount] = attr.ib(converter=ensure_tuple, default=())  # type: ignore
