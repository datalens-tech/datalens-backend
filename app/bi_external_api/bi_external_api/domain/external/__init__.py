from functools import cache
from typing import Type

from bi_external_api.attrs_model_mapper import ModelDescriptor
from bi_external_api.attrs_model_mapper.marshmallow import ModelMapperMarshmallow

from ...enums import ExtAPIType
from .avatar import (
    AvatarDef,
    AvatarJoinCondition,
    AvatarsConfig,
)
from .chart_coloring import (
    ColoringKind,
    ColorMount,
    DimensionColoring,
    FieldColoring,
    Gradient2,
    Gradient3,
    MeasureColoring,
    MeasureColoringSpec,
    MeasureColoringSpecKind,
    Thresholds2,
    Thresholds3,
)
from .chart_fields import (
    ChartFieldRef,
    ChartFieldSource,
    ChartFieldSourceKind,
    MeasureNames,
    MeasureValues,
)
from .chart_shape import (
    DimensionShaping,
    FieldShape,
    ShapeMount,
)
from .charts import (
    AdHocField,
    AreaChart,
    AreaChartNormalized,
    BarChart,
    BarChartNormalized,
    BaseAreaChart,
    BaseBarChart,
    BaseCircularChart,
    BaseColumnChart,
    Chart,
    ChartField,
    ChartFilter,
    ChartSort,
    ColumnChart,
    ColumnChartNormalized,
    DonutChart,
    FlatTable,
    Indicator,
    LineChart,
    PieChart,
    PivotTable,
    ScatterPlot,
    SortDirection,
    TreeMap,
    UnsupportedVisualization,
    Visualization,
    VisualizationType,
)
from .common import (
    EntryIDRef,
    EntryInfo,
    EntryKind,
    EntryRef,
    EntryRefKind,
    EntryWBRef,
    NameMapEntry,
    PlainSecret,
    Secret,
    SecretKind,
)
from .connection import (
    BaseCHYTConnection,
    CHYTConnection,
    CHYTUserAuthConnection,
    ClickHouseConnection,
    Connection,
    ConnectionKind,
    ConnectionSecret,
    PostgresConnection,
    RawSQLLevel,
    SQLConnection,
)
from .dash import (
    Dashboard,
    DashboardTab,
    DashboardTabItem,
    DashTabItemPlacement,
    IgnoredConnection,
)
from .dash_elements import (
    DashChartsContainer,
    DashElement,
    DashText,
    DashTitle,
    DashTitleTextSize,
    WidgetTab,
)
from .dash_elements_control import (
    ControlValueSource,
    ControlValueSourceDatasetField,
    ControlValueSourceManual,
    DashControl,
    DashControlDate,
    DashControlDateRange,
    DashControlExternal,
    DashControlGuided,
    DashControlMultiSelect,
    DashControlSelect,
    DashControlTextInput,
    MultiStringValue,
    SingleStringValue,
    Value,
    ValueKind,
    ValueSourceKind,
)
from .data_source import (
    CHYTTableListDataSourceSpec,
    CHYTTableRangeDataSourceSpec,
    DataSource,
    DataSourceKind,
    DataSourceSpec,
    ExtDataSourceProcessor,
    SchematizedTableDataSourceSpec,
    SubSelectDataSourceSpec,
    TableDataSourceSpec,
)
from .dataset_field import (
    Aggregation,
    CalcSpec,
    DatasetField,
    DirectCS,
    FieldKind,
    FieldType,
    FormulaCS,
    IDFormulaCS,
    ParameterCS,
)
from .dataset_main import Dataset
from .errors import (
    CommonError,
    EntryError,
    ErrWorkbookOp,
    ErrWorkbookOpClusterization,
)
from .filters import ComparisonOperation
from .model_tags import ExtModelTags
from .object_model import (
    ObjectParent,
    ObjectParentKind,
    ParentOrganization,
    ParentProject,
)
from .rpc import (
    AdviseDatasetFieldsRequest,
    AdviseDatasetFieldsResponse,
    ConnectionCreateRequest,
    ConnectionCreateResponse,
    ConnectionDeleteRequest,
    ConnectionDeleteResponse,
    ConnectionGetRequest,
    ConnectionGetResponse,
    ConnectionModifyRequest,
    ConnectionModifyResponse,
    EntryOperation,
    EntryOperationKind,
    FakeWorkbookCreateRequest,
    FakeWorkbookCreateResponse,
    ModificationPlan,
    TrueWorkbookCreateRequest,
    TrueWorkbookCreateResponse,
    WorkbookClusterizeRequest,
    WorkbookClusterizeResponse,
    WorkbookDeleteRequest,
    WorkbookDeleteResponse,
    WorkbookListRequest,
    WorkbookListResponse,
    WorkbookOpKind,
    WorkbookOpRequest,
    WorkbookOpResponse,
    WorkbookReadRequest,
    WorkbookReadResponse,
    WorkbookWriteRequest,
    WorkbookWriteResponse,
)
from .rpc_dc import (
    DCOpAdviseDatasetFieldsRequest,
    DCOpAdviseDatasetFieldsResponse,
    DCOpConnectionCreateRequest,
    DCOpConnectionCreateResponse,
    DCOpConnectionDeleteRequest,
    DCOpConnectionDeleteResponse,
    DCOpConnectionGetRequest,
    DCOpConnectionGetResponse,
    DCOpConnectionModifyRequest,
    DCOpConnectionModifyResponse,
    DCOpRequest,
    DCOpResponse,
    DCOpWorkbookCreateRequest,
    DCOpWorkbookCreateResponse,
    DCOpWorkbookDeleteRequest,
    DCOpWorkbookDeleteResponse,
    DCOpWorkbookGetRequest,
    DCOpWorkbookGetResponse,
    DCOpWorkbookListRequest,
    DCOpWorkbookListResponse,
    DCOpWorkbookModifyRequest,
    DCOpWorkbookModifyResponse,
)
from .workbook import (
    ChartInstance,
    ConnectionInstance,
    DashInstance,
    DatasetInstance,
    EntryInstance,
    WorkBook,
    WorkbookConnectionsOnly,
    WorkbookIndexItem,
)

_all_models = [
    AvatarDef,
    AvatarsConfig,
    AvatarJoinCondition,
    # chart_coloring.py
    ColoringKind,
    ColorMount,
    DimensionColoring,
    FieldColoring,
    Gradient2,
    Gradient3,
    MeasureColoring,
    MeasureColoringSpec,
    MeasureColoringSpecKind,
    Thresholds2,
    Thresholds3,
    # chart_fields.py
    ChartFieldRef,
    ChartFieldSource,
    ChartFieldSourceKind,
    ChartFilter,
    MeasureNames,
    MeasureValues,
    # chart_shape.py
    DimensionShaping,
    FieldShape,
    ShapeMount,
    # charts.py
    AdHocField,
    Chart,
    ChartField,
    ChartSort,
    BaseColumnChart,
    ColumnChart,
    ColumnChartNormalized,
    BaseAreaChart,
    AreaChart,
    AreaChartNormalized,
    BaseBarChart,
    BarChart,
    BarChartNormalized,
    ScatterPlot,
    BaseCircularChart,
    PieChart,
    DonutChart,
    FlatTable,
    Indicator,
    LineChart,
    TreeMap,
    PivotTable,
    SortDirection,
    Visualization,
    VisualizationType,
    UnsupportedVisualization,
    # common.py
    EntryIDRef,
    EntryInfo,
    EntryKind,
    EntryRef,
    EntryRefKind,
    EntryWBRef,
    NameMapEntry,
    PlainSecret,
    Secret,
    SecretKind,
    # connection.py
    BaseCHYTConnection,
    CHYTConnection,
    CHYTUserAuthConnection,
    ClickHouseConnection,
    Connection,
    ConnectionKind,
    ConnectionSecret,
    PostgresConnection,
    RawSQLLevel,
    SQLConnection,
    DashTabItemPlacement,
    DashboardTabItem,
    DashboardTab,
    IgnoredConnection,
    Dashboard,
    DashElement,
    DashTitle,
    DashTitleTextSize,
    DashText,
    WidgetTab,
    DashChartsContainer,
    ControlValueSource,
    ControlValueSourceDatasetField,
    ControlValueSourceManual,
    DashControl,
    DashControlDate,
    DashControlDateRange,
    DashControlExternal,
    DashControlGuided,
    DashControlMultiSelect,
    DashControlSelect,
    DashControlTextInput,
    MultiStringValue,
    SingleStringValue,
    Value,
    ValueKind,
    ValueSourceKind,
    CHYTTableListDataSourceSpec,
    CHYTTableRangeDataSourceSpec,
    DataSource,
    DataSourceKind,
    DataSourceSpec,
    ExtDataSourceProcessor,
    SchematizedTableDataSourceSpec,
    SubSelectDataSourceSpec,
    TableDataSourceSpec,
    # dataset_field.py
    Aggregation,
    CalcSpec,
    DatasetField,
    DirectCS,
    FieldKind,
    FieldType,
    FormulaCS,
    IDFormulaCS,
    ParameterCS,
    # dataset_main.py
    Dataset,
    # errors.py
    CommonError,
    EntryError,
    ErrWorkbookOp,
    ErrWorkbookOpClusterization,
    # filters.py
    ComparisonOperation,
    # model_tags.py
    ExtModelTags,
    # object_model.py
    ObjectParent,
    ObjectParentKind,
    ParentOrganization,
    ParentProject,
    # rpc.py
    AdviseDatasetFieldsRequest,
    AdviseDatasetFieldsResponse,
    ConnectionCreateRequest,
    ConnectionCreateResponse,
    ConnectionGetRequest,
    ConnectionGetResponse,
    ConnectionModifyRequest,
    ConnectionModifyResponse,
    ConnectionDeleteRequest,
    ConnectionDeleteResponse,
    TrueWorkbookCreateResponse,
    TrueWorkbookCreateRequest,
    EntryOperation,
    EntryOperationKind,
    ModificationPlan,
    WorkbookClusterizeRequest,
    WorkbookClusterizeResponse,
    FakeWorkbookCreateRequest,
    FakeWorkbookCreateResponse,
    WorkbookOpKind,
    WorkbookOpRequest,
    WorkbookOpResponse,
    WorkbookReadRequest,
    WorkbookReadResponse,
    WorkbookWriteRequest,
    WorkbookWriteResponse,
    WorkbookDeleteRequest,
    WorkbookDeleteResponse,
    WorkbookListRequest,
    WorkbookListResponse,
    # rpc.dc
    DCOpAdviseDatasetFieldsRequest,
    DCOpAdviseDatasetFieldsResponse,
    DCOpConnectionCreateRequest,
    DCOpConnectionCreateResponse,
    DCOpConnectionDeleteRequest,
    DCOpConnectionDeleteResponse,
    DCOpConnectionGetRequest,
    DCOpConnectionGetResponse,
    DCOpConnectionModifyRequest,
    DCOpConnectionModifyResponse,
    DCOpRequest,
    DCOpResponse,
    DCOpWorkbookCreateRequest,
    DCOpWorkbookCreateResponse,
    DCOpWorkbookDeleteRequest,
    DCOpWorkbookDeleteResponse,
    DCOpWorkbookGetRequest,
    DCOpWorkbookGetResponse,
    DCOpWorkbookModifyRequest,
    DCOpWorkbookModifyResponse,
    DCOpWorkbookListRequest,
    DCOpWorkbookListResponse,
    # workbook.py
    ChartInstance,
    ConnectionInstance,
    DashInstance,
    DatasetInstance,
    EntryInstance,
    WorkBook,
    WorkbookConnectionsOnly,
    WorkbookIndexItem,
]

# assert all([
#     g_val in _all_models
#     for g_name, g_val in globals().items()
#     if attr.has(g_val) and ModelDescriptor.has(g_val)
# ])


def keep_model(model_cls: Type, api_type: ExtAPIType) -> bool:
    if ModelDescriptor.has(model_cls):
        descriptor = ModelDescriptor.get_for_type(model_cls)

        if not descriptor.api_types and not descriptor.api_types_exclude:
            # model is omnipresent
            return True

        if descriptor.api_types:
            return api_type in descriptor.api_types

        if api_type not in descriptor.api_types_exclude:
            return True

    return False


@cache
def get_external_model_mapper(api_type: ExtAPIType) -> ModelMapperMarshmallow:
    mm = ModelMapperMarshmallow()
    to_register = [model_cls for model_cls in _all_models if keep_model(model_cls, api_type)]

    mm.register_models(to_register)
    return mm
