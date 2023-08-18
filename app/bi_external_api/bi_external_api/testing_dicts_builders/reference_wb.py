from typing import Any

from bi_external_api.testing_dicts_builders.chart import ChartJSONBuilderSingleDataset
from bi_external_api.testing_dicts_builders.dash import DashJSONBuilderSingleTab
from bi_external_api.testing_dicts_builders.dataset import SampleSuperStoreLightJSONBuilder


def build_reference_workbook(conn_name: str, fill_defaults: bool = False) -> dict[str, Any]:
    ds_name = "ds_sales"
    dataset_builder = SampleSuperStoreLightJSONBuilder(conn_name).with_fill_defaults(fill_defaults).with_full_data(True)

    dataset_builder = dataset_builder.add_field(
        dataset_builder.field_id_formula(
            "SUM([sales])",
            id="sum_sales",
            cast="integer",
        )
    ).do_add_default_fields()

    dataset_inst = dataset_builder.build_instance(ds_name)
    dataset = dataset_inst["dataset"]

    chart_builder = ChartJSONBuilderSingleDataset(
        ds_name=ds_name,
    ).with_source_dataset(dataset).with_fill_defaults(fill_defaults)

    def ds_f_ref(fid: str) -> dict[str, str]:
        return {
            "kind": "ref",
            "id": fid,
            **({"dataset_name": "ds_sales"} if fill_defaults else {}),
        }

    def c_mounts(mounts: dict[str, int]) -> list[dict]:
        return [{"value": value, "color_idx": idx} for value, idx in mounts.items()]

    chart_inst_list = [
        #
        chart_builder.add_formula_field(
            f_id="order_week",
            formula="DATETRUNC([date], 'week')",
            cast="date"
        ).with_visualization({
            "kind": "area_chart",
            "x": {"source": ds_f_ref("order_week")},
            "y": [
                {"source": ds_f_ref("sum_sales")},
            ],
            "sort": [],
            "coloring": {
                "kind": "dimension",
                "source": ds_f_ref("category"),
                "palette_id": "default-palette",
                "mounts": c_mounts({"Furniture": 19, "Office Supplies": 17, "Technology": 18}),
            }
        }).build_instance("ch_area_sales_per_week_per_cat"),
        #
        chart_builder.add_formula_field(
            f_id="order_month",
            formula="DATETRUNC([date], 'month')",
            cast="date"
        ).with_visualization({
            "kind": "area_chart_normalized",
            "x": {"source": ds_f_ref("order_month")},
            "y": [
                {"source": ds_f_ref("sum_sales")},
            ],
            "sort": [],
            "coloring": {
                "kind": "dimension",
                "source": ds_f_ref("category"),
                "palette_id": "default-palette",
                "mounts": c_mounts({"Furniture": 19, "Office Supplies": 17, "Technology": 18}),
            }
        }).build_instance("ch_area_normalized_sales_per_month_per_cat"),
        #
        chart_builder.with_visualization({
            "kind": "bar_chart",
            "y": [
                {"source": ds_f_ref("region")},
            ],
            "x": [
                {"source": ds_f_ref("sum_sales")},
            ],
            "sort": [{
                "source": ds_f_ref("region"),
                "direction": "ASC"
            }],
            "coloring": {
                "kind": "dimension",
                "source": ds_f_ref("category"),
                "palette_id": "default-palette",
                "mounts": c_mounts({"Furniture": 19, "Office Supplies": 17, "Technology": 18}),
            }
        }).build_instance("ch_bars_sales_per_region_and_category"),
        #
        chart_builder.with_visualization({
            "kind": "bar_chart_normalized",
            "y": [
                {"source": ds_f_ref("region")},
            ],
            "x": [
                {"source": ds_f_ref("sum_sales")},
            ],
            "sort": [{
                "source": ds_f_ref("region"),
                "direction": "ASC"
            }],
            "coloring": {
                "kind": "dimension",
                "source": ds_f_ref("category"),
                "palette_id": "default-palette",
                "mounts": c_mounts({"Furniture": 19, "Office Supplies": 17, "Technology": 18}),
            }
        }).build_instance("ch_bars_normalized_sales_per_region_and_category"),
        #
        chart_builder.with_visualization({
            "kind": "column_chart",
            "x": [
                {"source": ds_f_ref("region")},
            ],
            "y": [
                {"source": ds_f_ref("sum_sales")},
            ],
            "sort": [{
                "source": ds_f_ref("region"),
                "direction": "ASC"
            }],
            "coloring": None,
        }).build_instance("ch_columns_sales_per_region"),
        #
        chart_builder.with_visualization({
            "kind": "column_chart_normalized",
            "x": [
                {"source": ds_f_ref("region")},
            ],
            "y": [
                {"source": ds_f_ref("sum_sales")},
            ],
            "sort": [{
                "source": ds_f_ref("region"),
                "direction": "DESC"
            }],
            "coloring": {
                "kind": "dimension",
                "source": ds_f_ref("category"),
                "palette_id": "default-palette",
                "mounts": c_mounts({"Furniture": 19, "Office Supplies": 17, "Technology": 18}),
            },
        }).build_instance("ch_colums_normalized_sales_per_region_and_category"),
        #
        chart_builder.with_visualization({
            "kind": "donut_chart",
            "measures": {"source": ds_f_ref("sum_sales")},
            "sort": [],
            "coloring": {
                "source": ds_f_ref("region"),
                "palette_id": "default-palette",
                "mounts": c_mounts({"South": 16, "East": 7, "West": 17, "Central": 8}),
            },
        }).build_instance("ch_donut_sales_per_region"),
        #
        chart_builder.with_visualization({
            "kind": "pie_chart",
            "measures": {"source": ds_f_ref("sum_sales")},
            "sort": [],
            "coloring": {
                "source": ds_f_ref("region"),
                "palette_id": "default-palette",
                "mounts": [],
            },
        }).build_instance("ch_pie_sales_per_segment"),
        #
        chart_builder.with_visualization({
            "kind": "indicator",
            "field": {"source": ds_f_ref("sum_sales")},
        }).build_instance("ch_indicator_sales_sum_total"),
        #
        chart_builder.with_visualization({
            "kind": "treemap",
            "dimensions": [
                {"source": ds_f_ref("region")},
                {"source": ds_f_ref("category")},
            ],
            "measures": {"source": ds_f_ref("sum_sales")},
            "sort": [],
            "coloring": {
                "kind": "dimension",
                "source": ds_f_ref("category"),
                "palette_id": "default-palette",
                "mounts": c_mounts({"South": 16, "East": 7, "West": 17, "Central": 8})
            }
        }).build_instance("ch_treemap_sales_per_region"),
        #
        chart_builder.with_visualization({
            "kind": "flat_table",
            "columns": [
                {"source": ds_f_ref("sub_category")},
                {"source": ds_f_ref("segment")},
                {"source": ds_f_ref("sum_sales")}
            ],
            "sort": [
                {"source": ds_f_ref("sum_sales"), "direction": "DESC"}
            ],
            "coloring": None,
        }).build_instance("ch_flat_table_sales_per_subcategory_per_segment"),
        #
        chart_builder.add_formula_field(
            f_id="order_year",
            formula="DATETRUNC([date], 'year')",
            cast="date"
        ).with_visualization({
            "kind": "pivot_table",
            "columns": [
                {"source": ds_f_ref("category")},
            ],
            "rows": [
                {"source": ds_f_ref("order_year")},
                {"source": ds_f_ref("segment")},
            ],
            "measures": [
                {"source": ds_f_ref("sum_sales")}
            ],
            "sort": [
                {
                    "source": ds_f_ref("order_year"),
                    "direction": "DESC"
                }
            ],
            "coloring": {
                "source": ds_f_ref("sum_sales"),
                "spec": {
                    "kind": "gradient_2_points",
                    "palette": None,
                    "thresholds": None
                }
            }
        }).build_instance("ch_pivot_sum_sales_per_cat_seg_y"),
        #
        chart_builder.add_formula_field(
            f_id="order_month",
            formula="DATETRUNC([date], 'month')",
            cast="date"
        ).with_visualization({
            "kind": "line_chart",
            "x": {"source": ds_f_ref("order_month")},
            "y": [
                {"source": ds_f_ref("sum_sales")},
            ],
            "y2": [],
            "sort": [],
            "coloring": {
                "source": ds_f_ref("category"),
                "palette_id": "default-palette",
                "mounts": c_mounts({"Furniture": 19, "Office Supplies": 17, "Technology": 18}),
            }
        }).build_instance("ch_linear_sales_per_month"),
        #
        chart_builder.add_formula_field(
            f_id="max_profit",
            formula="MAX([profit])",
            cast="float"
        ).with_visualization({
            "kind": "scatter_plot",
            "x": {"source": ds_f_ref("region")},
            "y": {"source": ds_f_ref("sum_sales")},
            "sort": [],
            "coloring": {
                "kind": "dimension",
                "source": ds_f_ref("category"),
                "palette_id": None,
                "mounts": []
            },
            "points": {"source": ds_f_ref("segment")},
            "size": {"source": ds_f_ref("max_profit")},
        }).build_instance("ch_scatter_sales_per_region_per_segment_per_category"),
    ]

    dash_inst = DashJSONBuilderSingleTab([
        ch_inst["name"]
        for ch_inst in chart_inst_list
    ]).with_fill_defaults(fill_defaults).build_instance("main_dash")

    return dict(
        datasets=[dataset_inst],
        charts=chart_inst_list,
        dashboards=[dash_inst],
    )
