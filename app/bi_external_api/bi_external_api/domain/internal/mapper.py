from bi_external_api.attrs_model_mapper.marshmallow import ModelMapperMarshmallow

from . import (
    charts,
    dashboards,
    datasets,
)

internal_models_mapper = ModelMapperMarshmallow()

internal_models_mapper.register_models([
    # Literals
    datasets.DefaultValue,
    datasets.DefaultValueFloat,
    datasets.DefaultValueInteger,
    datasets.DefaultValueString,

    # Avatars
    datasets.Avatar,

    # Data source params
    datasets.DataSourceParamsSQL,
    datasets.DataSourceParamsSchematizedSQL,
    datasets.DataSourceParamsSubSQL,
    datasets.DataSourceParamsCHYTTableList,
    datasets.DataSourceParamsCHYTTableRange,

    # Data sources
    datasets.DataSource,
    datasets.DataSourceSQL,
    datasets.DataSourceSubSQL,
    datasets.DataSourceSchematizedSQL,

    datasets.DataSourceCHYTTable,
    datasets.DataSourceCHYTSubSelect,

    datasets.DataSourceCHYTUserAuthTable,
    datasets.DataSourceCHYTUserAuthSubSelect,

    datasets.DataSourceCHYTTableList,
    datasets.DataSourceCHYTUserAuthTableList,

    datasets.DataSourceCHYTTableRange,
    datasets.DataSourceCHYTUserAuthTableRange,

    datasets.DataSourcePGTable,
    datasets.DataSourcePGSubSQL,

    datasets.DataSourceClickHouseTable,
    datasets.DataSourceClickHouseSubSQL,

    # Actions
    datasets.Action,
    datasets.ActionDataSourceAdd,
    datasets.ActionAvatarAdd,
    datasets.ActionFieldAdd,

    # Dataset
    datasets.Dataset,

    # Fields
    datasets.ResultSchemaField,
    datasets.ResultSchemaFieldFull,

    # Charts
    charts.ChartActionField,
    charts.ChartAction,
    charts.ChartActionFieldAdd,
    charts.FilterOperation,
    charts.FilterValue,
    charts.Filter,
    charts.FieldFilter,
    charts.Visualization,
    charts.ColorConfig,
    charts.PeriodSettings,
    charts.NavigatorSettings,
    charts.ExtraSettings,
    charts.ChartField,
    charts.Placeholder,
    charts.Visualization,
    charts.DatasetFieldPartial,
    charts.Chart,

    # Dashboards
    dashboards.LayoutItem,
    dashboards.Connection,
    dashboards.Aliases,
    dashboards.Tab,
    dashboards.Dashboard,
    dashboards.TabItemData,
    dashboards.TabItemDataTitle,
    dashboards.TabItemDataText,
    dashboards.DatasetControlSource,
    dashboards.DatasetControlSourceSelect,
    dashboards.DatasetControlSourceDate,
    dashboards.DatasetControlSourceTextInput,
    dashboards.SelectorItem,
    dashboards.AcceptableDates,
    dashboards.ManualControlSource,
    dashboards.ManualControlSourceSelect,
    dashboards.ManualControlSourceDate,
    dashboards.ManualControlSourceTextInput,
    dashboards.ExternalControlSource,
    dashboards.ControlData,
    dashboards.DatasetBasedControlData,
    dashboards.ManualControlData,
    dashboards.ExternalControlData,
    dashboards.WidgetTabItem,
    dashboards.TabItemDataWidget,
    dashboards.TabItem,
    dashboards.ItemTitle,
    dashboards.ItemText,
    dashboards.ItemWidget,
    dashboards.ItemControl,
])
