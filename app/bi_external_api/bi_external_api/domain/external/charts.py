import enum
from collections.abc import Sequence
from typing import Optional, ClassVar

import attr

from bi_external_api.attrs_model_mapper import ModelDescriptor, AttribDescriptor
from bi_external_api.attrs_model_mapper.utils import MText
from bi_external_api.domain.utils import ensure_tuple
from .chart_coloring import FieldColoring, MeasureColoring, DimensionColoring
from .chart_fields import ChartFieldSource, ChartFieldRef
from .chart_shape import DimensionShaping
from .dash_elements_control import MultiStringValue
from .dataset_field import DatasetField
from .filters import ComparisonOperation
from .model_tags import ExtModelTags


class VisualizationType(enum.Enum):
    flat_table = enum.auto()
    pivot_table = enum.auto()
    indicator = enum.auto()
    column_chart = enum.auto()
    column_chart_normalized = enum.auto()
    line_chart = enum.auto()
    treemap = enum.auto()
    area_chart = enum.auto()
    area_chart_normalized = enum.auto()
    scatter_plot = enum.auto()
    bar_chart = enum.auto()
    bar_chart_normalized = enum.auto()
    pie_chart = enum.auto()
    donut_chart = enum.auto()
    unsupported = enum.auto()


@ModelDescriptor()
@attr.s(frozen=True)
class AdHocField:
    field: DatasetField = attr.ib()
    dataset_name: Optional[str] = attr.ib(
        default=None, kw_only=True,
        metadata=AttribDescriptor(
            description=MText(
                ru="Имя датасета, в котором нужно найти поле."
                   " Если чарт строится по одному датасету, имя можно не указывать.",
                en="The dataset name where you need to find a field."
                   " If the chart is built from a single dataset, you may skip specifying its name.",
            ),
            tags=frozenset({ExtModelTags.dataset_name}),
        ).to_meta(),
    )

    @property
    def strict_dataset_name(self) -> str:
        ds_name = self.dataset_name
        assert ds_name is not None
        return ds_name


@ModelDescriptor()
@attr.s(frozen=True)
class ChartField:
    source: ChartFieldSource = attr.ib()

    # Some customizations
    @classmethod
    def create_as_ref(cls, id: str, *, dataset_name: Optional[str] = None) -> "ChartField":
        return cls(source=ChartFieldRef(id=id, dataset_name=dataset_name))


@ModelDescriptor()
@attr.s(frozen=True)
class ChartFilter:
    field_ref: ChartFieldRef = attr.ib()
    operation: ComparisonOperation = attr.ib()
    value: MultiStringValue = attr.ib()


class SortDirection(enum.Enum):
    ASC = "ASC"
    DESC = "DESC"


@ModelDescriptor()
@attr.s(frozen=True)
class ChartSort:
    source: ChartFieldSource = attr.ib()
    direction: SortDirection = attr.ib()


@ModelDescriptor(
    is_abstract=True,
    children_type_discriminator_attr_name="kind",
    children_type_discriminator_aliases_attr_name="kind_aliases",
)
@attr.s(frozen=True, kw_only=True)
class Visualization:
    """
    Polymorphic form of visualization
    """
    kind: ClassVar[VisualizationType | str]
    kind_aliases: ClassVar[tuple[str, ...]] = tuple()

    # TODO FIX: BI-3005 Move to accessor
    def get_sort(self) -> Sequence[ChartSort]:
        return ()

    def get_dimension_shaping(self) -> Optional[DimensionShaping]:
        return None

    # TODO FIX: BI-3005 Move to accessor
    def get_coloring(self) -> Optional[FieldColoring]:
        return None


@ModelDescriptor(
    description=MText(
        ru="Показывает, что данный тип визуализации не может управляться через API, только UI."
           " Нельзя использовать при сохранении воркбуков. Выдается только при чтении.",
        en="Indicates that visualization can not be managed with API. Only UI."
           " Will not be accepted by modification method - readonly.",
    ),
)
@attr.s(frozen=True)
class UnsupportedVisualization(Visualization):
    kind = VisualizationType.unsupported

    message: str = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class Indicator(Visualization):
    kind = VisualizationType.indicator

    field: ChartField = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class FlatTable(Visualization):
    kind = VisualizationType.flat_table

    columns: Sequence[ChartField] = attr.ib(converter=ensure_tuple)

    sort: Sequence[ChartSort] = attr.ib(converter=ensure_tuple, default=())  # type: ignore
    coloring: Optional[MeasureColoring] = attr.ib(default=None)

    def get_sort(self) -> Sequence[ChartSort]:
        return self.sort

    def get_coloring(self) -> Optional[FieldColoring]:
        return self.coloring


@ModelDescriptor()
@attr.s(frozen=True)
class PivotTable(Visualization):
    kind = VisualizationType.pivot_table

    columns: Sequence[ChartField] = attr.ib(converter=ensure_tuple)
    rows: Sequence[ChartField] = attr.ib(converter=ensure_tuple)

    measures: Sequence[ChartField] = attr.ib(converter=ensure_tuple)

    sort: Sequence[ChartSort] = attr.ib(converter=ensure_tuple, default=())  # type: ignore
    coloring: Optional[MeasureColoring] = attr.ib(default=None)

    def get_sort(self) -> Sequence[ChartSort]:
        return self.sort

    def get_coloring(self) -> Optional[FieldColoring]:
        return self.coloring


@ModelDescriptor(is_abstract=True)
@attr.s(frozen=True, kw_only=True)
class BaseColumnChart(Visualization):
    x: Sequence[ChartField] = attr.ib(converter=ensure_tuple)  # note: max 2 items
    y: Sequence[ChartField] = attr.ib(converter=ensure_tuple)

    sort: Sequence[ChartSort] = attr.ib(converter=ensure_tuple, default=())  # type: ignore
    coloring: Optional[FieldColoring] = attr.ib(default=None)

    def get_sort(self) -> Sequence[ChartSort]:
        return self.sort

    def get_coloring(self) -> Optional[FieldColoring]:
        return self.coloring


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class ColumnChart(BaseColumnChart):
    kind = VisualizationType.column_chart
    kind_aliases = ("column",)

    x: Sequence[ChartField] = attr.ib(converter=ensure_tuple)  # note: max 2 items
    y: Sequence[ChartField] = attr.ib(converter=ensure_tuple)

    sort: Sequence[ChartSort] = attr.ib(converter=ensure_tuple, default=())  # type: ignore
    coloring: Optional[FieldColoring] = attr.ib(default=None)

    def get_sort(self) -> Sequence[ChartSort]:
        return self.sort

    def get_coloring(self) -> Optional[FieldColoring]:
        return self.coloring


@ModelDescriptor()
@attr.s(frozen=True)
class ColumnChartNormalized(BaseColumnChart):
    kind = VisualizationType.column_chart_normalized


@ModelDescriptor(is_abstract=True)
@attr.s(frozen=True)
class BaseBarChart(Visualization):
    y: Sequence[ChartField] = attr.ib(converter=ensure_tuple)  # note: max 2 items
    x: Sequence[ChartField] = attr.ib(converter=ensure_tuple)

    sort: Sequence[ChartSort] = attr.ib(converter=ensure_tuple, default=())  # type: ignore
    coloring: Optional[FieldColoring] = attr.ib(default=None)

    def get_sort(self) -> Sequence[ChartSort]:
        return self.sort

    def get_coloring(self) -> Optional[FieldColoring]:
        return self.coloring


@ModelDescriptor()
@attr.s(frozen=True)
class BarChart(BaseBarChart):
    kind = VisualizationType.bar_chart


@ModelDescriptor()
@attr.s(frozen=True)
class BarChartNormalized(BaseBarChart):
    kind = VisualizationType.bar_chart_normalized


@ModelDescriptor()
@attr.s(frozen=True)
class LineChart(Visualization):
    kind = VisualizationType.line_chart
    kind_aliases = ("linear_diagram",)

    x: ChartField = attr.ib()
    y: Sequence[ChartField] = attr.ib(converter=ensure_tuple)
    y2: Sequence[ChartField] = attr.ib(converter=ensure_tuple)

    sort: Sequence[ChartSort] = attr.ib(converter=ensure_tuple, default=())  # type: ignore
    coloring: Optional[DimensionColoring] = attr.ib(default=None)
    shaping: Optional[DimensionShaping] = attr.ib(default=None)

    def get_sort(self) -> Sequence[ChartSort]:
        return self.sort

    def get_coloring(self) -> Optional[FieldColoring]:
        return self.coloring

    def get_dimension_shaping(self) -> Optional[DimensionShaping]:
        return self.shaping


@ModelDescriptor(is_abstract=True)
@attr.s(frozen=True)
class BaseAreaChart(Visualization):
    x: ChartField = attr.ib()
    y: Sequence[ChartField] = attr.ib(converter=ensure_tuple)

    sort: Sequence[ChartSort] = attr.ib(converter=ensure_tuple, default=())  # type: ignore
    coloring: Optional[FieldColoring] = attr.ib(default=None)

    def get_sort(self) -> Sequence[ChartSort]:
        return self.sort

    def get_coloring(self) -> Optional[FieldColoring]:
        return self.coloring


@ModelDescriptor()
@attr.s(frozen=True)
class AreaChart(BaseAreaChart):
    kind = VisualizationType.area_chart


@ModelDescriptor()
@attr.s(frozen=True)
class AreaChartNormalized(BaseAreaChart):
    kind = VisualizationType.area_chart_normalized


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class ScatterPlot(Visualization):
    kind = VisualizationType.scatter_plot

    x: ChartField = attr.ib()
    y: ChartField = attr.ib()

    sort: Sequence[ChartSort] = attr.ib(converter=ensure_tuple, default=())  # type: ignore
    coloring: Optional[FieldColoring] = attr.ib(default=None)

    points: Optional[ChartField] = attr.ib(default=None)
    size: Optional[ChartField] = attr.ib(default=None)

    def get_sort(self) -> Sequence[ChartSort]:
        return self.sort

    def get_coloring(self) -> Optional[FieldColoring]:
        return self.coloring


@ModelDescriptor(is_abstract=True)
@attr.s(frozen=True, kw_only=True)
class BaseCircularChart(Visualization):
    measures: ChartField = attr.ib()

    sort: Sequence[ChartSort] = attr.ib(converter=ensure_tuple, default=())  # type: ignore
    coloring: DimensionColoring = attr.ib()

    def get_sort(self) -> Sequence[ChartSort]:
        return self.sort

    def get_coloring(self) -> DimensionColoring:
        return self.coloring


@ModelDescriptor()
@attr.s(frozen=True)
class PieChart(BaseCircularChart):
    kind = VisualizationType.pie_chart


@ModelDescriptor()
@attr.s(frozen=True)
class DonutChart(BaseCircularChart):
    kind = VisualizationType.donut_chart


@ModelDescriptor()
@attr.s(frozen=True)
class TreeMap(Visualization):
    kind = VisualizationType.treemap

    dimensions: Sequence[ChartField] = attr.ib(converter=ensure_tuple)
    measures: ChartField = attr.ib()

    sort: Sequence[ChartSort] = attr.ib(converter=ensure_tuple, default=())  # type: ignore
    coloring: Optional[FieldColoring] = attr.ib(default=None)

    def get_sort(self) -> Sequence[ChartSort]:
        return self.sort

    def get_coloring(self) -> Optional[FieldColoring]:
        return self.coloring


@ModelDescriptor()
@attr.s(frozen=True)
class Chart:
    datasets: Sequence[str] = attr.ib(
        metadata=AttribDescriptor(
            description=MText(
                ru="Список датасетов, по которым строится чарт.",
                en="List of datasets used to build the chart.",
            )
        ).to_meta(),
    )
    ad_hoc_fields: Sequence[AdHocField] = attr.ib(
        converter=ensure_tuple,
        metadata=AttribDescriptor(
            description=MText(
                ru="Дополнительные поля, необходимые для построения чарта.",
                en="Additional fields necessary to build the chart.",
            )
        ).to_meta(),
    )
    visualization: Visualization = attr.ib()
    filters: Sequence[ChartFilter] = attr.ib(
        converter=ensure_tuple,  # type: ignore
        metadata=AttribDescriptor(
            description=MText(
                ru="Позволяют делать выборку значений по измерениям или показателям.",
                en="Allows to limit dimension and measure values",
            )
        ).to_meta(),
        default=()
    )
