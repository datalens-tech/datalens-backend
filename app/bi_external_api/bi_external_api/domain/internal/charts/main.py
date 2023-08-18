from typing import Optional, Sequence, Any

import attr

from bi_constants.enums import WhereClauseOperation, BIType
from bi_external_api.attrs_model_mapper import ModelDescriptor
from bi_external_api.attrs_model_mapper.base import AttribDescriptor
from bi_external_api.structs.mappings import FrozenStrMapping
from .actions import ChartAction
from .enums import PlaceholderId, DatasetFieldType, VisualizationId, SortDirection
from ..datasets import ResultSchemaFieldFull
from ..dl_common import DatasetAPIBaseModel, EntryInstance, IntModelTags, EntryScope
from ...utils import ensure_tuple, ensure_tuple_of_tuples


@ModelDescriptor()
@attr.s(frozen=True)
class FilterOperation:
    code: WhereClauseOperation = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True)
class FilterValue:
    value: list[str] = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class Filter:
    value: FilterValue = attr.ib()
    operation: FilterOperation = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class FieldFilter(ResultSchemaFieldFull):
    datasetId: str = attr.ib(metadata=AttribDescriptor(tags=frozenset({IntModelTags.dataset_id})).to_meta())
    disabled: Optional[str] = None
    filter: Filter


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class ColorConfig(DatasetAPIBaseModel):
    fieldGuid: Optional[str] = attr.ib(default=None)
    coloredByMeasure: Optional[bool] = attr.ib(default=None)

    # For dimensions coloring
    palette: Optional[str] = attr.ib(default=None)
    mountedColors: Optional[FrozenStrMapping[str]] = attr.ib(default=None)
    # For measures coloring

    gradientPalette: Optional[str] = attr.ib(default=None)
    gradientMode: Optional[str] = attr.ib(default=None)

    thresholdsMode: Optional[str] = attr.ib(default=None)

    leftThreshold: Optional[str] = attr.ib(default=None)
    middleThreshold: Optional[str] = attr.ib(default=None)
    rightThreshold: Optional[str] = attr.ib(default=None)

    ignore_not_declared_fields = True


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class ShapeConfig(DatasetAPIBaseModel):
    fieldGuid: Optional[str] = attr.ib(default=None)
    mountedShapes: Optional[FrozenStrMapping[str]] = attr.ib(default=None)

    ignore_not_declared_fields = True


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class PeriodSettings:
    type: str = attr.ib()
    value: str = attr.ib()
    period: str = attr.ib()


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class NavigatorSettings:
    navigatorMode: str
    isNavigatorAvailable: bool
    selectedLines: Sequence[str] = attr.ib(converter=ensure_tuple)
    linesMode: str
    periodSettings: PeriodSettings


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class ExtraSettings(DatasetAPIBaseModel):
    # title: Optional[str] = None
    # titleMode: Optional[ShowHide] = None
    # legendMode: Optional[ShowHide] = None
    # metricFontSize: Optional[str] = None
    # metricFontColor: Optional[str] = None
    # tooltipSum: Optional[OnOff] = None
    # limit: Optional[int] = None
    # pagination: Optional[OnOff] = None
    # navigatorMode: Optional[str] = None
    # navigatorSeriesName: Optional[str] = None
    # totals: Optional[OnOff] = None
    # pivotFallback: Optional[OnOff] = None
    # feed: Optional[str] = None
    # navigatorSettings: Optional[NavigatorSettings] = None

    ignore_not_declared_fields = True


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class ChartField(DatasetAPIBaseModel):
    # TODO FIX: Frontend team was ask to load this field during chart processing. Will be removed in near future
    data_type: Optional[BIType]
    # originalTitle?: string;
    # fields?: V2Field[];
    # TODO FIX: Frontend team was ask to load this field during chart processing. Will be removed in near future
    type: Optional[DatasetFieldType]
    title: str
    guid: Optional[str] = attr.ib(default=None)
    # formatting?: V2Formatting;
    # format: string;
    # labelMode: string;
    datasetId: Optional[str] = attr.ib(
        default=None,
        metadata=AttribDescriptor(tags=frozenset({IntModelTags.dataset_id})).to_meta()
    )
    # dateMode: string;
    # fakeTitle?: string;
    # source?: string;
    # datasetName?: string;
    # hideLabelMode: string;

    ignore_not_declared_fields = True


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class ChartFieldSort(ChartField):
    direction: Optional[SortDirection] = attr.ib(default=None)  # Field may no be presented in config


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class Placeholder(DatasetAPIBaseModel):
    id: PlaceholderId = attr.ib(metadata=AttribDescriptor(enum_by_value=True).to_meta())
    # settings: Optional[Any] = attr.ib()
    items: Sequence[ChartField] = attr.ib(converter=ensure_tuple)

    ignore_not_declared_fields = True


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True)
class Visualization(DatasetAPIBaseModel):
    ignore_not_declared_fields = True

    id: VisualizationId = attr.ib()
    highchartsId: Optional[str] = attr.ib()
    # selectedLayerId:  Optional[str]
    # layers: Any
    placeholders: Sequence[Placeholder] = attr.ib(converter=ensure_tuple)

    @highchartsId.default
    def default_highcharts_id(self) -> Optional[str]:
        # https://a.yandex-team.ru/arcadia/data-ui/datalens/src/ui/units/ql/constants/visualizations.tsx?rev=r9891434#L20
        return {
            VisualizationId.area100p: 'area',
            VisualizationId.column100p: 'column',
            VisualizationId.bar100p: 'bar',
            VisualizationId.donut: 'pie',
        }.get(self.id)


@ModelDescriptor()
@attr.s(frozen=True, kw_only=True, auto_attribs=True)
class DatasetFieldPartial(DatasetAPIBaseModel):
    ignore_not_declared_fields = True

    title: str
    guid: str


@ModelDescriptor()
@attr.s(kw_only=True, frozen=True)
class Chart(DatasetAPIBaseModel):
    ignore_not_declared_fields = True

    type: str = attr.ib(default="datalens")
    version: str = attr.ib(default="2")

    shapes: Optional[Sequence[ChartField]] = attr.ib(default=(), converter=ensure_tuple)  # type: ignore
    colors: Optional[Sequence[ChartField]] = attr.ib(default=(), converter=ensure_tuple)  # type: ignore
    sort: Optional[Sequence[ChartFieldSort]] = attr.ib(default=(), converter=ensure_tuple)  # type: ignore
    filters: Sequence[FieldFilter] = attr.ib(default=(), converter=ensure_tuple)  # type: ignore

    colorsConfig: Optional[ColorConfig] = attr.ib(default=None, metadata=AttribDescriptor(
        skip_none_on_dump=True
    ).to_meta())
    shapesConfig: Optional[ShapeConfig] = attr.ib(default=None)
    extraSettings: ExtraSettings = attr.ib(factory=ExtraSettings)

    visualization: Optional[Visualization] = attr.ib(default=None)
    js: Optional[str] = attr.ib(default=None)

    datasetsIds: Optional[Sequence[str]] = attr.ib(
        converter=ensure_tuple,
        metadata=AttribDescriptor(tags=frozenset({IntModelTags.dataset_id})).to_meta(),
        default=None,
    )
    # datasetsLocalFields: Sequence[Sequence[ResultSchemaField]] = attr.ib()
    datasetsPartialFields: Optional[Sequence[Sequence[DatasetFieldPartial]]] = attr.ib(  # type: ignore
        converter=ensure_tuple_of_tuples,
        default=None,
    )

    updates: Sequence[ChartAction] = attr.ib(factory=tuple, converter=ensure_tuple)

    @classmethod
    def adopt_json_before_deserialization(cls, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        # BI-4508
        # Workaround for accidental int version in charts config
        # May be it can be removed later. Ask FE team later.
        if data.get("version") == 8:
            return {
                **data,
                "version": "8"
            }
        # In other cases - no any modifications
        return None


@attr.s()
class ChartInstance(EntryInstance):
    SCOPE = EntryScope.widget

    chart: Chart = attr.ib()
