import enum
from typing import Sequence

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor
from bi_external_api.domain.utils import ensure_tuple
from .chart_fields import ChartFieldSource


class FieldShape(enum.Enum):
    # Means `not specified`
    auto = 'auto'
    # Highcharts line styles
    dash = 'Dash'
    dash_dot = 'DashDot'
    dot = 'Dot'
    long_dash = 'LongDash'
    long_dash_dot = 'LongDashDot'
    long_dash_dot_dot = 'LongDashDotDot'
    short_dash = 'ShortDash'
    short_dash_dot = 'ShortDash'
    short_dash_dot_dot = 'ShortDashDotDot'
    short_dot = 'ShortDot'
    solid = 'Solid'


@ModelDescriptor()
@attr.s(frozen=True, auto_attribs=True)
class ShapeMount:
    value: str
    shape: FieldShape


@ModelDescriptor()
@attr.s(frozen=True)
class DimensionShaping:
    source: ChartFieldSource = attr.ib()
    mounts: Sequence[ShapeMount] = attr.ib(converter=ensure_tuple, default=())  # type: ignore
