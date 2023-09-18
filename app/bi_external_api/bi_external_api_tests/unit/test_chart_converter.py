import itertools
from typing import (
    ClassVar,
    Optional,
    Sequence,
    Type,
)

import attr
import pytest
import shortuuid

from bi_external_api.attrs_model_mapper import Processor
from bi_external_api.attrs_model_mapper.field_processor import FieldMeta
from bi_external_api.converter.charts.chart_converter import BaseChartConverter
from bi_external_api.converter.charts.utils import (
    ChartActionConverter,
    convert_field_type_dataset_to_chart,
)
from bi_external_api.converter.converter_ctx import ConverterContext
from bi_external_api.converter.workbook import WorkbookContext
from bi_external_api.domain import external as ext
from bi_external_api.domain.external import get_external_model_mapper
from bi_external_api.domain.internal import (
    charts,
    datasets,
)
from bi_external_api.enums import ExtAPIType
from bi_external_api.structs.mappings import FrozenStrMapping
from dl_constants.enums import (
    AggregationFunction,
    BIType,
    CalcMode,
    FieldType,
    ManagedBy,
    WhereClauseOperation,
)

from .conftest import (
    PG_1_DS,
    PG_CONN,
)

COMMON_WB_CONTEXT = WorkbookContext(
    connections=[PG_CONN],
    datasets=[PG_1_DS],
    charts=[],
    dashboards=[],
)


@attr.s(auto_attribs=True, frozen=True)
class AdHocPack:
    ext_ds_field: ext.DatasetField
    rs_field: datasets.ResultSchemaFieldFull

    @classmethod
    def create(
        cls,
        ext_f: ext.DatasetField,
        data_type: BIType,
        field_type: FieldType,
        int_agg: AggregationFunction = AggregationFunction.none,
        guid_formula: Optional[str] = None,
    ) -> "AdHocPack":
        cs = ext_f.calc_spec

        calc_mode: CalcMode.formula
        avatar_id: Optional[str]
        source: str
        formula: str

        if isinstance(cs, ext.FormulaCS):
            calc_mode = CalcMode.formula
            avatar_id = None
            source = ""
            formula = cs.formula
            guid_formula = None  # int should still return guid formula, but we don't expect incoming request to have it
        elif isinstance(cs, ext.IDFormulaCS):
            calc_mode = CalcMode.formula
            avatar_id = None
            source = ""
            assert guid_formula == cs.formula
            formula = ""
        # TODO FIX: BI-3366 Add test cases with direct fields
        elif isinstance(cs, ext.DirectCS):
            calc_mode = CalcMode.direct
            avatar_id = "--replace_me--"
            source = cs.field_name
            formula = ""
        else:
            raise AssertionError()
        return AdHocPack(
            ext_ds_field=ext_f,
            rs_field=datasets.ResultSchemaFieldFull(
                guid=ext_f.id,
                title=ext_f.title,
                description=ext_f.description or "",
                #
                calc_mode=calc_mode,
                avatar_id=avatar_id,
                source=source,
                formula=formula,
                guid_formula=guid_formula,
                aggregation=int_agg,
                type=field_type,
                cast=data_type,
                data_type=data_type,
                initial_data_type=data_type,
                #
                strict=False,
                hidden=ext_f.hidden,
                has_auto_aggregation=False,
                lock_aggregation=False,
                aggregation_locked=False,
                autoaggregated=False,
                managed_by=ManagedBy.user,
                virtual=False,
                valid=True,
            ),
        )


@attr.s()
class ChartTestCase:
    name: str = attr.ib()
    ext_chart: ext.Chart = attr.ib(repr=False)
    int_chart: charts.Chart = attr.ib(repr=False)
    map_ds_id_updated_dataset: Optional[dict[str, datasets.Dataset]] = attr.ib(repr=False, default=None)
    api_type: ExtAPIType = attr.ib(default=ExtAPIType.CORE)

    use_id_formula: bool = attr.ib(default=None)

    CF_TITLE_PLACEHOLDER: ClassVar[str] = shortuuid.uuid()
    CF_DS_ID_PLACEHOLDER: ClassVar[str] = shortuuid.uuid()

    @classmethod
    def cf_measure_names(self) -> charts.ChartField:
        return charts.ChartField(
            type=charts.DatasetFieldType.PSEUDO,
            data_type=BIType.string,
            title="Measure Names",
        )

    @classmethod
    def cf_measure_values(self) -> charts.ChartField:
        return charts.ChartField(
            type=charts.DatasetFieldType.PSEUDO,
            data_type=BIType.float,
            title="Measure Values",
        )

    @classmethod
    def cf_placeholder(cls, field_id: str) -> charts.ChartField:
        return charts.ChartField(
            guid=field_id,
            title=cls.CF_TITLE_PLACEHOLDER,
            datasetId=cls.CF_DS_ID_PLACEHOLDER,
            #
            data_type=None,
            type=None,
        )

    @classmethod
    def chart_filter_placeholder(
        cls,
        field_id: str,
        filter_value: list[str],
        data_type: BIType,
        field_type: FieldType,
        filter_operation: charts.FilterOperation,
    ) -> charts.FieldFilter:
        return charts.FieldFilter(
            guid=field_id,
            title=field_id,
            datasetId="pg_conn_1_ds_ID",
            #
            type=field_type,
            cast=data_type,
            data_type=data_type,
            initial_data_type=data_type,
            #
            filter=charts.Filter(value=charts.FilterValue(filter_value), operation=filter_operation),
            #
            strict=False,
            hidden=False,
            has_auto_aggregation=False,
            lock_aggregation=False,
            aggregation_locked=False,
            autoaggregated=False,
            managed_by=ManagedBy.user,
            virtual=False,
            valid=True,
            aggregation=AggregationFunction.none,
            calc_mode=CalcMode.direct,
            description=f"id='{field_id}'",
            formula="",
            avatar_id="the_avatar",
            source=field_id,
        )

    @classmethod
    def chart_sort_placeholder(cls, field_id: str, direction: Optional[charts.SortDirection]) -> charts.ChartFieldSort:
        return charts.ChartFieldSort(
            guid=field_id,
            title=cls.CF_TITLE_PLACEHOLDER,
            datasetId=cls.CF_DS_ID_PLACEHOLDER,
            #
            data_type=None,
            type=None,
            #
            direction=direction,
        )

    @classmethod
    def chart_sort_measure_names(cls, direction: Optional[charts.SortDirection]) -> charts.ChartFieldSort:
        return charts.ChartFieldSort(
            direction=direction,
            **attr.asdict(cls.cf_measure_names(), recurse=False),
        )

    @attr.s
    class _DsIdSetter(Processor):
        ds_inst: datasets.DatasetInstance = attr.ib()

        def _should_process(self, meta: FieldMeta) -> bool:
            return issubclass(meta.clz, charts.ChartField)

        def _process_single_object(self, obj: charts.ChartField, meta: FieldMeta) -> Optional[charts.ChartField]:
            if obj is None:
                return None
            # Do not postprocess measure names/values
            if obj.type is charts.DatasetFieldType.PSEUDO:
                return obj

            ret = obj
            ds_field = self.ds_inst.dataset.get_field_by_id(obj.guid)

            if obj.datasetId == ChartTestCase.CF_DS_ID_PLACEHOLDER:
                ret = attr.evolve(ret, datasetId=self.ds_inst.summary.id)
            if obj.title == ChartTestCase.CF_TITLE_PLACEHOLDER:
                ret = attr.evolve(ret, title=ds_field.title)
            if obj.data_type is None:
                ret = attr.evolve(ret, data_type=ds_field.data_type)
            if obj.type is None:
                ret = attr.evolve(ret, type=convert_field_type_dataset_to_chart(ds_field.type))

            return ret

    @classmethod
    def ahp_formula_avg_amount(cls, f_id: str, calc_spec_clz: Type = ext.FormulaCS) -> AdHocPack:
        return AdHocPack.create(
            ext.DatasetField(
                id=f_id,
                title=f"The {f_id}",
                calc_spec=calc_spec_clz(formula="AVG([amount])"),
                cast=ext.FieldType.float,
                description=None,
            ),
            data_type=BIType.float,
            field_type=FieldType.MEASURE,
            guid_formula="AVG([amount])",  # since test setup uses same names for field id and title
        )

    @classmethod
    def ahp_formula_countd_date(cls, f_id: str, calc_spec_clz: Type = ext.FormulaCS) -> AdHocPack:
        return AdHocPack.create(
            ext.DatasetField(
                id=f_id,
                title=f"The {f_id}",
                calc_spec=calc_spec_clz(formula="COUNTD([date])"),
                cast=ext.FieldType.integer,
                description=None,
            ),
            data_type=BIType.integer,
            field_type=FieldType.MEASURE,
            guid_formula="COUNTD([date])",
        )

    @classmethod
    def add_fields_to_ds(
        cls, ds: datasets.Dataset, f_seq: Sequence[datasets.ResultSchemaFieldFull]
    ) -> datasets.Dataset:
        new_rs = list(itertools.chain(ds.result_schema, f_seq))

        assert len(new_rs) == len({f.guid for f in new_rs})
        assert len(new_rs) == len({f.title for f in new_rs})

        return attr.evolve(ds, result_schema=new_rs)

    @classmethod
    def normal_pg_1_ds_case(
        cls,
        name: str,
        *,
        ad_hoc_packs: Sequence[AdHocPack] = (),
        ext_vis: ext.Visualization,
        ext_filters: Sequence[ext.ChartFilter] = (),
        int_vis: charts.Visualization,
        int_filters: Sequence[charts.FieldFilter] = (),
        int_sort: Sequence[charts.ChartFieldSort] = (),
        int_colors: Sequence[charts.ChartField] = (),
        int_colors_config: Optional[charts.ColorConfig] = None,
        int_shapes: Sequence[charts.ChartField] = (),
        int_shapes_config: Optional[charts.ShapeConfig] = None,
        use_id_formula: bool = False,
    ):
        ds_inst = PG_1_DS

        int_updates = [
            ChartActionConverter.convert_action_add_field_dataset_to_chart(
                datasets.ActionFieldAdd(field=ahp.rs_field.to_writable_result_schema()),
                dataset_id=ds_inst.summary.id,
            )
            for ahp in ad_hoc_packs
        ]

        ds_with_applies_actions = cls.add_fields_to_ds(ds_inst.dataset, [ahp.rs_field for ahp in ad_hoc_packs])

        return ChartTestCase(
            name,
            ext_chart=ext.Chart(
                datasets=[ds_inst.summary.name],
                ad_hoc_fields=[
                    ext.AdHocField(field=ahp.ext_ds_field, dataset_name=ds_inst.summary.name) for ahp in ad_hoc_packs
                ],
                visualization=ext_vis,
                filters=ext_filters,
            ),
            int_chart=cls._DsIdSetter(attr.evolve(ds_inst, dataset=ds_with_applies_actions)).process(
                charts.Chart(
                    visualization=int_vis,
                    filters=int_filters,
                    sort=int_sort,
                    colors=int_colors,
                    colorsConfig=int_colors_config,
                    shapes=int_shapes,
                    shapesConfig=int_shapes_config,
                    datasetsIds=[ds_inst.summary.id],
                    datasetsPartialFields=[
                        [
                            charts.DatasetFieldPartial(title=rs_field.title, guid=rs_field.guid)
                            for rs_field in ds_inst.dataset.result_schema
                        ]
                        + [
                            charts.DatasetFieldPartial(guid=upd.field.guid, title=upd.field.title)
                            for upd in int_updates
                        ]
                    ],
                    updates=int_updates,
                )
            ),
            map_ds_id_updated_dataset={ds_inst.summary.id: ds_with_applies_actions} if ad_hoc_packs else None,
            use_id_formula=use_id_formula,
        )


CASES: list[ChartTestCase] = [
    ChartTestCase.normal_pg_1_ds_case(
        "simple_flat_table_no_updates",  # region
        ext_vis=ext.FlatTable(
            columns=[
                ext.ChartField.create_as_ref("date"),
                ext.ChartField.create_as_ref("amount_sum"),
            ]
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.flatTable,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.FlatTableColumns,
                    items=(
                        ChartTestCase.cf_placeholder("date"),
                        ChartTestCase.cf_placeholder("amount_sum"),
                    ),
                ),
            ),
        ),
    ),
    ChartTestCase.normal_pg_1_ds_case(
        "simple_flat_table_with_filter",  # region
        ext_vis=ext.FlatTable(
            columns=[
                ext.ChartField.create_as_ref("date"),
                ext.ChartField.create_as_ref("amount_sum"),
            ]
        ),
        ext_filters=[
            ext.ChartFilter(
                ext.ChartFieldRef(id="date"),
                operation=ext.ComparisonOperation.EQ,
                value=ext.MultiStringValue(["2023-06-12T00:00:00.000Z"]),
            )
        ],
        int_vis=charts.Visualization(
            id=charts.VisualizationId.flatTable,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.FlatTableColumns,
                    items=(
                        ChartTestCase.cf_placeholder("date"),
                        ChartTestCase.cf_placeholder("amount_sum"),
                    ),
                ),
            ),
        ),
        int_filters=[
            ChartTestCase.chart_filter_placeholder(
                field_id="date",
                filter_value=["2023-06-12T00:00:00.000Z"],
                filter_operation=charts.FilterOperation(WhereClauseOperation.EQ),
                field_type=FieldType.DIMENSION,
                data_type=BIType.date,
            )
        ],
    ),
    ChartTestCase.normal_pg_1_ds_case(
        "simple_flat_table_with_3_point_gradient",  # region
        ext_vis=ext.FlatTable(
            columns=[
                ext.ChartField.create_as_ref("date"),
            ],
            coloring=ext.MeasureColoring(source=ext.ChartFieldRef("amount_sum"), spec=ext.Gradient3()),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.flatTable,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.FlatTableColumns, items=(ChartTestCase.cf_placeholder("date"),)
                ),
            ),
        ),
        int_colors=[ChartTestCase.cf_placeholder("amount_sum")],
        int_colors_config=charts.ColorConfig(
            fieldGuid="amount_sum",
            coloredByMeasure=True,
            gradientMode="3-point",
            thresholdsMode="auto",
        ),
    ),
    ChartTestCase.normal_pg_1_ds_case(
        "simple_flat_table_with_3_point_gradient_with_thresholds",  # region
        ext_vis=ext.FlatTable(
            columns=[
                ext.ChartField.create_as_ref("date"),
            ],
            coloring=ext.MeasureColoring(
                source=ext.ChartFieldRef("amount_sum"),
                spec=ext.Gradient3(
                    palette="green-yellow-red",
                    thresholds=ext.Thresholds3(left=10.1, middle=50, right=100),
                ),
            ),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.flatTable,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.FlatTableColumns, items=(ChartTestCase.cf_placeholder("date"),)
                ),
            ),
        ),
        int_colors=[ChartTestCase.cf_placeholder("amount_sum")],
        int_colors_config=charts.ColorConfig(
            fieldGuid="amount_sum",
            coloredByMeasure=True,
            gradientMode="3-point",
            gradientPalette="green-yellow-red",
            thresholdsMode="manual",
            leftThreshold="10.1",
            middleThreshold="50",
            rightThreshold="100",
        ),
    ),
    ChartTestCase.normal_pg_1_ds_case(
        "simple_flat_table_with_2_point_gradient_with_thresholds",  # region
        ext_vis=ext.FlatTable(
            columns=[
                ext.ChartField.create_as_ref("date"),
            ],
            coloring=ext.MeasureColoring(
                source=ext.ChartFieldRef("amount_sum"),
                spec=ext.Gradient2(
                    palette="red-green",
                    thresholds=ext.Thresholds2(left=10, right=100),
                ),
            ),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.flatTable,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.FlatTableColumns, items=(ChartTestCase.cf_placeholder("date"),)
                ),
            ),
        ),
        int_colors=[ChartTestCase.cf_placeholder("amount_sum")],
        int_colors_config=charts.ColorConfig(
            fieldGuid="amount_sum",
            coloredByMeasure=True,
            gradientMode="2-point",
            gradientPalette="red-green",
            thresholdsMode="manual",
            leftThreshold="10",
            rightThreshold="100",
        ),
    ),
    # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "flat_with_ad_hoc_formula",  # region
        ad_hoc_packs=[ChartTestCase.ahp_formula_avg_amount("amount_avg")],
        ext_vis=ext.FlatTable(
            columns=[
                ext.ChartField.create_as_ref("date"),
                ext.ChartField.create_as_ref("amount_sum"),
                ext.ChartField.create_as_ref("amount_avg"),
            ]
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.flatTable,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.FlatTableColumns,
                    items=(
                        ChartTestCase.cf_placeholder("date"),
                        ChartTestCase.cf_placeholder("amount_sum"),
                        ChartTestCase.cf_placeholder("amount_avg"),
                    ),
                ),
            ),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "flat_with_ad_hoc_formula_guid",  # region
        ad_hoc_packs=[ChartTestCase.ahp_formula_avg_amount("amount_avg", calc_spec_clz=ext.IDFormulaCS)],
        use_id_formula=True,
        ext_vis=ext.FlatTable(
            columns=[
                ext.ChartField.create_as_ref("date"),
                ext.ChartField.create_as_ref("amount_sum"),
                ext.ChartField.create_as_ref("amount_avg"),
            ]
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.flatTable,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.FlatTableColumns,
                    items=(
                        ChartTestCase.cf_placeholder("date"),
                        ChartTestCase.cf_placeholder("amount_sum"),
                        ChartTestCase.cf_placeholder("amount_avg"),
                    ),
                ),
            ),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "flat_with_ad_hoc_formula_with_coloring",  # region
        ad_hoc_packs=[ChartTestCase.ahp_formula_avg_amount("amount_avg")],
        ext_vis=ext.FlatTable(
            columns=[
                ext.ChartField.create_as_ref("date"),
                ext.ChartField.create_as_ref("amount_avg"),
            ],
            coloring=ext.MeasureColoring(source=ext.ChartFieldRef("amount_sum"), spec=ext.Gradient2()),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.flatTable,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.FlatTableColumns,
                    items=(
                        ChartTestCase.cf_placeholder("date"),
                        ChartTestCase.cf_placeholder("amount_avg"),
                    ),
                ),
            ),
        ),
        int_colors=[ChartTestCase.cf_placeholder("amount_sum")],
        int_colors_config=charts.ColorConfig(
            fieldGuid="amount_sum",
            coloredByMeasure=True,
            gradientMode="2-point",
            thresholdsMode="auto",
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "pivot_table_with_updates_with_sort",  # region
        ad_hoc_packs=[ChartTestCase.ahp_formula_avg_amount("amount_avg")],
        ext_vis=ext.PivotTable(
            columns=(ext.ChartField.create_as_ref("customer"),),
            rows=(ext.ChartField.create_as_ref("position"),),
            measures=(
                ext.ChartField.create_as_ref("amount_avg"),
                ext.ChartField.create_as_ref("amount_sum"),
            ),
            sort=(
                ext.ChartSort(
                    source=ext.ChartFieldRef("date"),
                    direction=ext.SortDirection.DESC,
                ),
            ),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.pivotTable,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.PivotTableColumns,
                    items=(ChartTestCase.cf_placeholder("customer"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Rows,
                    items=(ChartTestCase.cf_placeholder("position"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Measures,
                    items=(
                        ChartTestCase.cf_placeholder("amount_avg"),
                        ChartTestCase.cf_placeholder("amount_sum"),
                    ),
                ),
            ),
        ),
        int_sort=[
            ChartTestCase.chart_sort_placeholder("date", charts.SortDirection.DESC),
        ],
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "pivot_table_with_updates_with_sort_with_coloring",  # region
        ad_hoc_packs=[ChartTestCase.ahp_formula_avg_amount("amount_avg")],
        ext_vis=ext.PivotTable(
            columns=(ext.ChartField.create_as_ref("customer"),),
            rows=(ext.ChartField.create_as_ref("position"),),
            measures=(ext.ChartField.create_as_ref("amount_sum"),),
            sort=(
                ext.ChartSort(
                    source=ext.ChartFieldRef("date"),
                    direction=ext.SortDirection.DESC,
                ),
            ),
            coloring=ext.MeasureColoring(source=ext.ChartFieldRef("amount_avg"), spec=ext.Gradient2()),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.pivotTable,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.PivotTableColumns,
                    items=(ChartTestCase.cf_placeholder("customer"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Rows,
                    items=(ChartTestCase.cf_placeholder("position"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Measures,
                    items=(ChartTestCase.cf_placeholder("amount_sum"),),
                ),
            ),
        ),
        int_colors=[ChartTestCase.cf_placeholder("amount_avg")],
        int_colors_config=charts.ColorConfig(
            fieldGuid="amount_avg",
            coloredByMeasure=True,
            gradientMode="2-point",
            thresholdsMode="auto",
        ),
        int_sort=[
            ChartTestCase.chart_sort_placeholder("date", charts.SortDirection.DESC),
        ],
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "simple_indicator",  # region
        ext_vis=ext.Indicator(
            field=ext.ChartField.create_as_ref("date"),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.metric,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.Measures,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
            ),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "measure_names",  # region
        ext_vis=ext.Indicator(
            field=ext.ChartField(source=ext.MeasureNames()),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.metric,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.Measures,
                    items=(ChartTestCase.cf_measure_names(),),
                ),
            ),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "measure_values",  # region
        ext_vis=ext.Indicator(
            field=ext.ChartField(source=ext.MeasureValues()),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.metric,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.Measures,
                    items=(ChartTestCase.cf_measure_values(),),
                ),
            ),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "indicator_with_updates",  # region
        ad_hoc_packs=[ChartTestCase.ahp_formula_countd_date("countd")],
        ext_vis=ext.Indicator(
            field=ext.ChartField.create_as_ref("countd"),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.metric,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.Measures,
                    items=(ChartTestCase.cf_placeholder("countd"),),
                ),
            ),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "columns_with_updates_with_sort",  # region
        ad_hoc_packs=[ChartTestCase.ahp_formula_avg_amount("amount_avg")],
        ext_vis=ext.ColumnChart(
            x=[ext.ChartField.create_as_ref("date"), ext.ChartField.create_as_ref("customer")],
            y=[ext.ChartField.create_as_ref("amount_avg")],
            sort=[
                ext.ChartSort(source=ext.ChartFieldRef("date"), direction=ext.SortDirection.DESC),
                ext.ChartSort(source=ext.MeasureNames(), direction=ext.SortDirection.ASC),
            ],
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.column,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(
                        ChartTestCase.cf_placeholder("date"),
                        ChartTestCase.cf_placeholder("customer"),
                    ),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("amount_avg"),),
                ),
            ),
        ),
        int_sort=(
            ChartTestCase.chart_sort_placeholder("date", charts.SortDirection.DESC),
            ChartTestCase.chart_sort_measure_names(charts.SortDirection.ASC),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "columns_with_colors_by_dimension",  # region
        ext_vis=ext.ColumnChart(
            x=[ext.ChartField.create_as_ref("date")],
            y=[ext.ChartField.create_as_ref("amount_sum")],
            coloring=ext.DimensionColoring(
                source=ext.ChartFieldRef("customer"),
                palette_id=None,
            ),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.column,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("amount_sum"),),
                ),
            ),
        ),
        int_colors=(ChartTestCase.cf_placeholder("customer"),),
        int_colors_config=charts.ColorConfig(
            fieldGuid="customer",
            coloredByMeasure=False,
            mountedColors=FrozenStrMapping({}),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "columns_with_colors_by_dimension_with_mounts",  # region
        ext_vis=ext.ColumnChart(
            x=[ext.ChartField.create_as_ref("date")],
            y=[ext.ChartField.create_as_ref("amount_sum")],
            coloring=ext.DimensionColoring(
                source=ext.ChartFieldRef("customer"),
                palette_id="p1",
                mounts=[
                    ext.ColorMount(value="Vasya", color_idx=0),
                    ext.ColorMount(value="Petya", color_idx=1),
                ],
            ),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.column,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("amount_sum"),),
                ),
            ),
        ),
        int_colors=(ChartTestCase.cf_placeholder("customer"),),
        int_colors_config=charts.ColorConfig(
            fieldGuid="customer",
            coloredByMeasure=False,
            mountedColors=FrozenStrMapping(
                {
                    "Vasya": "0",
                    "Petya": "1",
                }
            ),
            palette="p1",
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "columns_with_updates_with_coloring_by_measure_names",  # region
        ad_hoc_packs=[ChartTestCase.ahp_formula_avg_amount("amount_avg")],
        ext_vis=ext.ColumnChart(
            x=[ext.ChartField.create_as_ref("date")],
            y=[ext.ChartField.create_as_ref("amount_avg"), ext.ChartField.create_as_ref("amount_sum")],
            coloring=ext.DimensionColoring(source=ext.MeasureNames(), palette_id=None),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.column,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(
                        ChartTestCase.cf_placeholder("amount_avg"),
                        ChartTestCase.cf_placeholder("amount_sum"),
                    ),
                ),
            ),
        ),
        int_colors=[ChartTestCase.cf_measure_names()],
        int_colors_config=charts.ColorConfig(
            fieldGuid=None,
            coloredByMeasure=False,
            palette=None,
            mountedColors=FrozenStrMapping({}),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "normalized_columns_with_updates_with_sort",  # region
        ad_hoc_packs=[ChartTestCase.ahp_formula_avg_amount("amount_avg")],
        ext_vis=ext.ColumnChartNormalized(
            x=[ext.ChartField.create_as_ref("date"), ext.ChartField.create_as_ref("customer")],
            y=[ext.ChartField.create_as_ref("amount_avg")],
            sort=[
                ext.ChartSort(source=ext.ChartFieldRef("date"), direction=ext.SortDirection.DESC),
                ext.ChartSort(source=ext.MeasureNames(), direction=ext.SortDirection.ASC),
            ],
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.column100p,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(
                        ChartTestCase.cf_placeholder("date"),
                        ChartTestCase.cf_placeholder("customer"),
                    ),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("amount_avg"),),
                ),
            ),
        ),
        int_sort=(
            ChartTestCase.chart_sort_placeholder("date", charts.SortDirection.DESC),
            ChartTestCase.chart_sort_measure_names(charts.SortDirection.ASC),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "normalized_columns_with_colors_by_dimension",  # region
        ext_vis=ext.ColumnChartNormalized(
            x=[ext.ChartField.create_as_ref("date")],
            y=[ext.ChartField.create_as_ref("amount_sum")],
            coloring=ext.DimensionColoring(
                source=ext.ChartFieldRef("customer"),
                palette_id=None,
            ),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.column100p,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("amount_sum"),),
                ),
            ),
        ),
        int_colors=(ChartTestCase.cf_placeholder("customer"),),
        int_colors_config=charts.ColorConfig(
            fieldGuid="customer",
            coloredByMeasure=False,
            mountedColors=FrozenStrMapping({}),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "normalized_columns_with_colors_by_dimension_with_mounts",  # region
        ext_vis=ext.ColumnChartNormalized(
            x=[ext.ChartField.create_as_ref("date")],
            y=[ext.ChartField.create_as_ref("amount_sum")],
            coloring=ext.DimensionColoring(
                source=ext.ChartFieldRef("customer"),
                palette_id="p1",
                mounts=[
                    ext.ColorMount(value="Vasya", color_idx=0),
                    ext.ColorMount(value="Petya", color_idx=1),
                ],
            ),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.column100p,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("amount_sum"),),
                ),
            ),
        ),
        int_colors=(ChartTestCase.cf_placeholder("customer"),),
        int_colors_config=charts.ColorConfig(
            fieldGuid="customer",
            coloredByMeasure=False,
            mountedColors=FrozenStrMapping(
                {
                    "Vasya": "0",
                    "Petya": "1",
                }
            ),
            palette="p1",
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "normalized_columns_with_updates_with_coloring_by_measure_names",  # region
        ad_hoc_packs=[ChartTestCase.ahp_formula_avg_amount("amount_avg")],
        ext_vis=ext.ColumnChartNormalized(
            x=[ext.ChartField.create_as_ref("date")],
            y=[ext.ChartField.create_as_ref("amount_avg"), ext.ChartField.create_as_ref("amount_sum")],
            coloring=ext.DimensionColoring(source=ext.MeasureNames(), palette_id=None),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.column100p,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(
                        ChartTestCase.cf_placeholder("amount_avg"),
                        ChartTestCase.cf_placeholder("amount_sum"),
                    ),
                ),
            ),
        ),
        int_colors=[ChartTestCase.cf_measure_names()],
        int_colors_config=charts.ColorConfig(
            fieldGuid=None,
            coloredByMeasure=False,
            palette=None,
            mountedColors=FrozenStrMapping({}),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "linear_dia_with_updates_with_shaping",  # region
        ad_hoc_packs=[ChartTestCase.ahp_formula_avg_amount("amount_avg")],
        ext_vis=ext.LineChart(
            x=ext.ChartField.create_as_ref("date"),
            y=[ext.ChartField.create_as_ref("amount_avg"), ext.ChartField.create_as_ref("amount_sum")],
            y2=[],
            shaping=ext.DimensionShaping(source=ext.MeasureNames()),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.line,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(
                        ChartTestCase.cf_placeholder("amount_avg"),
                        ChartTestCase.cf_placeholder("amount_sum"),
                    ),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y2,
                    items=(),
                ),
            ),
        ),
        int_shapes=[ChartTestCase.cf_measure_names()],
        int_shapes_config=charts.ShapeConfig(
            fieldGuid=None,
            mountedShapes=FrozenStrMapping({}),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "linear_dia_with_shaping_with_mounts",  # region
        ext_vis=ext.LineChart(
            x=ext.ChartField.create_as_ref("date"),
            y=[ext.ChartField.create_as_ref("amount_sum")],
            y2=[],
            shaping=ext.DimensionShaping(
                source=ext.ChartFieldRef("customer"),
                mounts=[
                    ext.ShapeMount(value="Vasya", shape=ext.FieldShape.dash),
                    ext.ShapeMount(value="Petya", shape=ext.FieldShape.short_dash),
                ],
            ),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.line,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("amount_sum"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y2,
                    items=(),
                ),
            ),
        ),
        int_shapes=[ChartTestCase.cf_placeholder("customer")],
        int_shapes_config=charts.ShapeConfig(
            fieldGuid="customer",
            mountedShapes=FrozenStrMapping(
                {
                    "Vasya": "Dash",
                    "Petya": "ShortDash",
                }
            ),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "pie_chart_basic",  # region
        ext_vis=ext.PieChart(
            measures=ext.ChartField.create_as_ref("amount_sum"),
            coloring=ext.DimensionColoring(
                source=ext.ChartFieldRef("customer"),
                palette_id="p1",
                mounts=[
                    ext.ColorMount(value="Vasya", color_idx=0),
                    ext.ColorMount(value="Petya", color_idx=1),
                ],
            ),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.pie,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.Dimensions, items=(ChartTestCase.cf_placeholder("customer"),)
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Measures, items=(ChartTestCase.cf_placeholder("amount_sum"),)
                ),
            ),
        ),
        # int_colors=(),
        int_colors_config=charts.ColorConfig(
            fieldGuid="customer",
            coloredByMeasure=False,
            mountedColors=FrozenStrMapping(
                {
                    "Vasya": "0",
                    "Petya": "1",
                }
            ),
            palette="p1",
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "donut_chart_basic",  # region
        ext_vis=ext.PieChart(
            measures=ext.ChartField.create_as_ref("amount_sum"),
            coloring=ext.DimensionColoring(
                source=ext.ChartFieldRef("customer"),
                palette_id="p1",
                mounts=[
                    ext.ColorMount(value="Vasya", color_idx=0),
                    ext.ColorMount(value="Petya", color_idx=1),
                ],
            ),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.pie,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.Dimensions, items=(ChartTestCase.cf_placeholder("customer"),)
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Measures, items=(ChartTestCase.cf_placeholder("amount_sum"),)
                ),
            ),
        ),
        # int_colors=(),
        int_colors_config=charts.ColorConfig(
            fieldGuid="customer",
            coloredByMeasure=False,
            mountedColors=FrozenStrMapping(
                {
                    "Vasya": "0",
                    "Petya": "1",
                }
            ),
            palette="p1",
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "tree_dia_basic",  # region
        ext_vis=ext.TreeMap(
            dimensions=[ext.ChartField.create_as_ref("position")],
            measures=ext.ChartField.create_as_ref("amount_sum"),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.treemap,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.Dimensions, items=(ChartTestCase.cf_placeholder("position"),)
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Measures, items=(ChartTestCase.cf_placeholder("amount_sum"),)
                ),
            ),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "tree_dia_with_coloring",  # region
        ext_vis=ext.TreeMap(
            dimensions=[ext.ChartField.create_as_ref("position")],
            measures=ext.ChartField.create_as_ref("amount_sum"),
            coloring=ext.DimensionColoring(
                source=ext.ChartFieldRef("position"),
                palette_id=None,
            ),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.treemap,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.Dimensions, items=(ChartTestCase.cf_placeholder("position"),)
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Measures, items=(ChartTestCase.cf_placeholder("amount_sum"),)
                ),
            ),
        ),
        int_colors=(ChartTestCase.cf_placeholder("position"),),
        int_colors_config=charts.ColorConfig(
            fieldGuid="position",
            coloredByMeasure=False,
            mountedColors=FrozenStrMapping({}),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "scatter_plot_basic",  # region
        ext_vis=ext.ScatterPlot(
            x=ext.ChartField.create_as_ref("position"),
            y=ext.ChartField.create_as_ref("date"),
            points=ext.ChartField.create_as_ref("customer"),
            size=ext.ChartField.create_as_ref("amount"),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.scatter,
            placeholders=(
                charts.Placeholder(id=charts.PlaceholderId.X, items=(ChartTestCase.cf_placeholder("position"),)),
                charts.Placeholder(id=charts.PlaceholderId.Y, items=(ChartTestCase.cf_placeholder("date"),)),
                charts.Placeholder(id=charts.PlaceholderId.Points, items=(ChartTestCase.cf_placeholder("customer"),)),
                charts.Placeholder(id=charts.PlaceholderId.Size, items=(ChartTestCase.cf_placeholder("amount"),)),
            ),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "bars_with_updates_with_sort",  # region
        ad_hoc_packs=[ChartTestCase.ahp_formula_avg_amount("amount_avg")],
        ext_vis=ext.BarChart(
            y=[ext.ChartField.create_as_ref("amount_avg")],
            x=[ext.ChartField.create_as_ref("date"), ext.ChartField.create_as_ref("customer")],
            sort=[
                ext.ChartSort(source=ext.ChartFieldRef("date"), direction=ext.SortDirection.DESC),
                ext.ChartSort(source=ext.MeasureNames(), direction=ext.SortDirection.ASC),
            ],
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.bar,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("amount_avg"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(
                        ChartTestCase.cf_placeholder("date"),
                        ChartTestCase.cf_placeholder("customer"),
                    ),
                ),
            ),
        ),
        int_sort=(
            ChartTestCase.chart_sort_placeholder("date", charts.SortDirection.DESC),
            ChartTestCase.chart_sort_measure_names(charts.SortDirection.ASC),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "bars_with_colors_by_dimension",  # region
        ext_vis=ext.BarChart(
            y=[ext.ChartField.create_as_ref("date")],
            x=[ext.ChartField.create_as_ref("amount_sum")],
            coloring=ext.DimensionColoring(
                source=ext.ChartFieldRef("customer"),
                palette_id=None,
            ),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.bar,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(ChartTestCase.cf_placeholder("amount_sum"),),
                ),
            ),
        ),
        int_colors=(ChartTestCase.cf_placeholder("customer"),),
        int_colors_config=charts.ColorConfig(
            fieldGuid="customer",
            coloredByMeasure=False,
            mountedColors=FrozenStrMapping({}),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "bars_with_colors_by_dimension_with_mounts",  # region
        ext_vis=ext.BarChart(
            y=[ext.ChartField.create_as_ref("date")],
            x=[ext.ChartField.create_as_ref("amount_sum")],
            coloring=ext.DimensionColoring(
                source=ext.ChartFieldRef("customer"),
                palette_id="p1",
                mounts=[
                    ext.ColorMount(value="Vasya", color_idx=0),
                    ext.ColorMount(value="Petya", color_idx=1),
                ],
            ),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.bar,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(ChartTestCase.cf_placeholder("amount_sum"),),
                ),
            ),
        ),
        int_colors=(ChartTestCase.cf_placeholder("customer"),),
        int_colors_config=charts.ColorConfig(
            fieldGuid="customer",
            coloredByMeasure=False,
            mountedColors=FrozenStrMapping(
                {
                    "Vasya": "0",
                    "Petya": "1",
                }
            ),
            palette="p1",
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "bars_with_updates_with_coloring_by_measure_names",  # region
        ad_hoc_packs=[ChartTestCase.ahp_formula_avg_amount("amount_avg")],
        ext_vis=ext.BarChart(
            y=[ext.ChartField.create_as_ref("date")],
            x=[ext.ChartField.create_as_ref("amount_avg"), ext.ChartField.create_as_ref("amount_sum")],
            coloring=ext.DimensionColoring(source=ext.MeasureNames(), palette_id=None),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.bar,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(
                        ChartTestCase.cf_placeholder("amount_avg"),
                        ChartTestCase.cf_placeholder("amount_sum"),
                    ),
                ),
            ),
        ),
        int_colors=[ChartTestCase.cf_measure_names()],
        int_colors_config=charts.ColorConfig(
            fieldGuid=None,
            coloredByMeasure=False,
            palette=None,
            mountedColors=FrozenStrMapping({}),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "normalized_bars_with_updates_with_sort",  # region
        ad_hoc_packs=[ChartTestCase.ahp_formula_avg_amount("amount_avg")],
        ext_vis=ext.BarChartNormalized(
            y=[ext.ChartField.create_as_ref("amount_avg")],
            x=[ext.ChartField.create_as_ref("date"), ext.ChartField.create_as_ref("customer")],
            sort=[
                ext.ChartSort(source=ext.ChartFieldRef("date"), direction=ext.SortDirection.DESC),
                ext.ChartSort(source=ext.MeasureNames(), direction=ext.SortDirection.ASC),
            ],
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.bar100p,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("amount_avg"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(
                        ChartTestCase.cf_placeholder("date"),
                        ChartTestCase.cf_placeholder("customer"),
                    ),
                ),
            ),
        ),
        int_sort=(
            ChartTestCase.chart_sort_placeholder("date", charts.SortDirection.DESC),
            ChartTestCase.chart_sort_measure_names(charts.SortDirection.ASC),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "normalized_bars_with_colors_by_dimension",  # region
        ext_vis=ext.BarChartNormalized(
            y=[ext.ChartField.create_as_ref("date")],
            x=[ext.ChartField.create_as_ref("amount_sum")],
            coloring=ext.DimensionColoring(
                source=ext.ChartFieldRef("customer"),
                palette_id=None,
            ),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.bar100p,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(ChartTestCase.cf_placeholder("amount_sum"),),
                ),
            ),
        ),
        int_colors=(ChartTestCase.cf_placeholder("customer"),),
        int_colors_config=charts.ColorConfig(
            fieldGuid="customer",
            coloredByMeasure=False,
            mountedColors=FrozenStrMapping({}),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "normalized_bars_with_colors_by_dimension_with_mounts",  # region
        ext_vis=ext.BarChartNormalized(
            y=[ext.ChartField.create_as_ref("date")],
            x=[ext.ChartField.create_as_ref("amount_sum")],
            coloring=ext.DimensionColoring(
                source=ext.ChartFieldRef("customer"),
                palette_id="p1",
                mounts=[
                    ext.ColorMount(value="Vasya", color_idx=0),
                    ext.ColorMount(value="Petya", color_idx=1),
                ],
            ),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.bar100p,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(ChartTestCase.cf_placeholder("amount_sum"),),
                ),
            ),
        ),
        int_colors=(ChartTestCase.cf_placeholder("customer"),),
        int_colors_config=charts.ColorConfig(
            fieldGuid="customer",
            coloredByMeasure=False,
            mountedColors=FrozenStrMapping(
                {
                    "Vasya": "0",
                    "Petya": "1",
                }
            ),
            palette="p1",
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "normalized_bars_with_updates_with_coloring_by_measure_names",  # region
        ad_hoc_packs=[ChartTestCase.ahp_formula_avg_amount("amount_avg")],
        ext_vis=ext.BarChartNormalized(
            y=[ext.ChartField.create_as_ref("date")],
            x=[ext.ChartField.create_as_ref("amount_avg"), ext.ChartField.create_as_ref("amount_sum")],
            coloring=ext.DimensionColoring(source=ext.MeasureNames(), palette_id=None),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.bar100p,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(
                        ChartTestCase.cf_placeholder("amount_avg"),
                        ChartTestCase.cf_placeholder("amount_sum"),
                    ),
                ),
            ),
        ),
        int_colors=[ChartTestCase.cf_measure_names()],
        int_colors_config=charts.ColorConfig(
            fieldGuid=None,
            coloredByMeasure=False,
            palette=None,
            mountedColors=FrozenStrMapping({}),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "area_chart_with_updates_with_sort",  # region
        ad_hoc_packs=[ChartTestCase.ahp_formula_avg_amount("amount_avg")],
        ext_vis=ext.AreaChart(
            x=ext.ChartField.create_as_ref("date"),
            y=[ext.ChartField.create_as_ref("amount_avg")],
            sort=[
                ext.ChartSort(source=ext.ChartFieldRef("date"), direction=ext.SortDirection.DESC),
                ext.ChartSort(source=ext.MeasureNames(), direction=ext.SortDirection.ASC),
            ],
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.area,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("amount_avg"),),
                ),
            ),
        ),
        int_sort=(
            ChartTestCase.chart_sort_placeholder("date", charts.SortDirection.DESC),
            ChartTestCase.chart_sort_measure_names(charts.SortDirection.ASC),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "area_chart_with_colors_by_dimension",  # region
        ext_vis=ext.AreaChart(
            x=ext.ChartField.create_as_ref("date"),
            y=[ext.ChartField.create_as_ref("amount_sum")],
            coloring=ext.DimensionColoring(
                source=ext.ChartFieldRef("customer"),
                palette_id=None,
            ),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.area,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("amount_sum"),),
                ),
            ),
        ),
        int_colors=(ChartTestCase.cf_placeholder("customer"),),
        int_colors_config=charts.ColorConfig(
            fieldGuid="customer",
            coloredByMeasure=False,
            mountedColors=FrozenStrMapping({}),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "area_chart_with_colors_by_dimension_with_mounts",  # region
        ext_vis=ext.AreaChart(
            x=ext.ChartField.create_as_ref("date"),
            y=[ext.ChartField.create_as_ref("amount_sum")],
            coloring=ext.DimensionColoring(
                source=ext.ChartFieldRef("customer"),
                palette_id="p1",
                mounts=[
                    ext.ColorMount(value="Vasya", color_idx=0),
                    ext.ColorMount(value="Petya", color_idx=1),
                ],
            ),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.area,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("amount_sum"),),
                ),
            ),
        ),
        int_colors=(ChartTestCase.cf_placeholder("customer"),),
        int_colors_config=charts.ColorConfig(
            fieldGuid="customer",
            coloredByMeasure=False,
            mountedColors=FrozenStrMapping(
                {
                    "Vasya": "0",
                    "Petya": "1",
                }
            ),
            palette="p1",
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "area_chart_with_updates_with_coloring_by_measure_names",  # region
        ad_hoc_packs=[ChartTestCase.ahp_formula_avg_amount("amount_avg")],
        ext_vis=ext.AreaChart(
            x=ext.ChartField.create_as_ref("date"),
            y=[ext.ChartField.create_as_ref("amount_avg"), ext.ChartField.create_as_ref("amount_sum")],
            coloring=ext.DimensionColoring(source=ext.MeasureNames(), palette_id=None),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.area,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(
                        ChartTestCase.cf_placeholder("amount_avg"),
                        ChartTestCase.cf_placeholder("amount_sum"),
                    ),
                ),
            ),
        ),
        int_colors=[ChartTestCase.cf_measure_names()],
        int_colors_config=charts.ColorConfig(
            fieldGuid=None,
            coloredByMeasure=False,
            palette=None,
            mountedColors=FrozenStrMapping({}),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "normalized_area_chart_with_updates_with_sort",  # region
        ad_hoc_packs=[ChartTestCase.ahp_formula_avg_amount("amount_avg")],
        ext_vis=ext.AreaChartNormalized(
            x=ext.ChartField.create_as_ref("date"),
            y=[ext.ChartField.create_as_ref("amount_avg")],
            sort=[
                ext.ChartSort(source=ext.ChartFieldRef("date"), direction=ext.SortDirection.DESC),
                ext.ChartSort(source=ext.MeasureNames(), direction=ext.SortDirection.ASC),
            ],
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.area100p,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("amount_avg"),),
                ),
            ),
        ),
        int_sort=(
            ChartTestCase.chart_sort_placeholder("date", charts.SortDirection.DESC),
            ChartTestCase.chart_sort_measure_names(charts.SortDirection.ASC),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "normalized_area_chart_with_colors_by_dimension",  # region
        ext_vis=ext.AreaChartNormalized(
            x=ext.ChartField.create_as_ref("date"),
            y=[ext.ChartField.create_as_ref("amount_sum")],
            coloring=ext.DimensionColoring(
                source=ext.ChartFieldRef("customer"),
                palette_id=None,
            ),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.area100p,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("amount_sum"),),
                ),
            ),
        ),
        int_colors=(ChartTestCase.cf_placeholder("customer"),),
        int_colors_config=charts.ColorConfig(
            fieldGuid="customer",
            coloredByMeasure=False,
            mountedColors=FrozenStrMapping({}),
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "normalized_area_chart_with_colors_by_dimension_with_mounts",  # region
        ext_vis=ext.AreaChartNormalized(
            x=ext.ChartField.create_as_ref("date"),
            y=[ext.ChartField.create_as_ref("amount_sum")],
            coloring=ext.DimensionColoring(
                source=ext.ChartFieldRef("customer"),
                palette_id="p1",
                mounts=[
                    ext.ColorMount(value="Vasya", color_idx=0),
                    ext.ColorMount(value="Petya", color_idx=1),
                ],
            ),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.area100p,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(ChartTestCase.cf_placeholder("amount_sum"),),
                ),
            ),
        ),
        int_colors=(ChartTestCase.cf_placeholder("customer"),),
        int_colors_config=charts.ColorConfig(
            fieldGuid="customer",
            coloredByMeasure=False,
            mountedColors=FrozenStrMapping(
                {
                    "Vasya": "0",
                    "Petya": "1",
                }
            ),
            palette="p1",
        ),
    ),  # endregion
    ChartTestCase.normal_pg_1_ds_case(
        "normalized_area_chart_with_updates_with_coloring_by_measure_names",  # region
        ad_hoc_packs=[ChartTestCase.ahp_formula_avg_amount("amount_avg")],
        ext_vis=ext.AreaChartNormalized(
            x=ext.ChartField.create_as_ref("date"),
            y=[ext.ChartField.create_as_ref("amount_avg"), ext.ChartField.create_as_ref("amount_sum")],
            coloring=ext.DimensionColoring(source=ext.MeasureNames(), palette_id=None),
        ),
        int_vis=charts.Visualization(
            id=charts.VisualizationId.area100p,
            placeholders=(
                charts.Placeholder(
                    id=charts.PlaceholderId.X,
                    items=(ChartTestCase.cf_placeholder("date"),),
                ),
                charts.Placeholder(
                    id=charts.PlaceholderId.Y,
                    items=(
                        ChartTestCase.cf_placeholder("amount_avg"),
                        ChartTestCase.cf_placeholder("amount_sum"),
                    ),
                ),
            ),
        ),
        int_colors=[ChartTestCase.cf_measure_names()],
        int_colors_config=charts.ColorConfig(
            fieldGuid=None,
            coloredByMeasure=False,
            palette=None,
            mountedColors=FrozenStrMapping({}),
        ),
    ),  # endregion
]


@pytest.mark.parametrize("case", CASES, ids=[case.name for case in CASES])
def test_chart_converter_ext_to_int(case: ChartTestCase):
    converter = BaseChartConverter(COMMON_WB_CONTEXT, ConverterContext(use_id_formula=case.use_id_formula))

    defaulted_ext_chart = converter.fill_defaults(case.ext_chart)
    actual_int_chart = converter.convert_chart_ext_to_int(
        defaulted_ext_chart,
        datasets_with_applied_actions=case.map_ds_id_updated_dataset,
    )

    assert actual_int_chart == case.int_chart


@pytest.mark.parametrize("case", CASES, ids=[case.name for case in CASES])
def test_chart_converter_int_to_ext(case: ChartTestCase):
    converter = BaseChartConverter(COMMON_WB_CONTEXT, ConverterContext(use_id_formula=case.use_id_formula))

    defaulted_ext_chart = converter.fill_defaults(case.ext_chart)
    actual_ext_chart = converter.convert_chart_int_to_ext(case.int_chart)

    assert actual_ext_chart == defaulted_ext_chart


@pytest.mark.parametrize("case", CASES, ids=[case.name for case in CASES])
def test_ext_chart_serialization(case: ChartTestCase):
    converter = BaseChartConverter(COMMON_WB_CONTEXT, ConverterContext())
    chart = converter.fill_defaults(case.ext_chart)
    mapper = get_external_model_mapper(case.api_type)
    schema = mapper.get_or_create_schema_for_attrs_class(type(chart))()
    data = schema.dump(chart)
    assert schema.load(data) == chart

    for alias in chart.visualization.kind_aliases:
        data["visualization"]["kind"] = alias
        assert schema.load(data) == chart


# TODO: add cases with guid formulas in ad-hoc fields
