from dl_api_client.dsmaker.pivot_utils import check_pivot_response
from dl_api_client.dsmaker.primitives import PivotTotals
from dl_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import PivotRole
from dl_constants.internal_constants import (
    DIMENSION_NAME_TITLE,
    MEASURE_NAME_TITLE,
)


class TestPivotWithSimpleTotals(DefaultApiTestBase):
    def test_main_totals(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
            },
        )

        pivot_abs = check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category"],
            rows=["region"],
            measures=["sales sum"],
            simple_totals=PivotTotals(
                rows=[PivotTotals.item(level=0)],
                columns=[PivotTotals.item(level=0)],
            ),
        )
        col_titles = pivot_abs.get_flat_column_headers()
        row_titles = pivot_abs.get_flat_row_headers()
        assert col_titles == ["Furniture", "Office Supplies", "Technology", ""]
        assert row_titles == ["Central", "East", "South", "West", ""]

    def test_with_totals_flag(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
            },
        )

        pivot_abs = check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category"],
            rows=["region"],
            measures=["sales sum"],
            with_totals=True,
        )
        col_titles = pivot_abs.get_flat_column_headers()
        row_titles = pivot_abs.get_flat_row_headers()
        assert col_titles == ["Furniture", "Office Supplies", "Technology", ""]
        assert row_titles == ["Central", "East", "South", "West", ""]

    def test_corner_case_totals(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
            },
        )

        check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category"],
            rows=[],
            measures=["sales sum"],
            simple_totals=PivotTotals(
                rows=[],
                columns=[PivotTotals.item(level=0)],
            ),
            custom_pivot_legend_check=[
                ("category", PivotRole.pivot_column),
                ("sales sum", PivotRole.pivot_measure),
                (MEASURE_NAME_TITLE, PivotRole.pivot_row),
                (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
            ],
        )

        check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=[],
            rows=["category"],
            measures=["sales sum"],
            simple_totals=PivotTotals(
                rows=[PivotTotals.item(level=0)],
                columns=[],
            ),
            custom_pivot_legend_check=[
                ("category", PivotRole.pivot_row),
                ("sales sum", PivotRole.pivot_measure),
                (MEASURE_NAME_TITLE, PivotRole.pivot_column),
                (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
            ],
        )

        check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category"],
            rows=["region"],
            measures=[],
            simple_totals=PivotTotals(
                rows=[PivotTotals.item(level=0)],
                columns=[PivotTotals.item(level=0)],
            ),
        )

    def test_multi_measure_corner_case_totals_flag(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([profit])",
            },
        )

        check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category"],
            rows=[],
            measures=["sales sum", "profit sum"],
            with_totals=True,
            custom_pivot_legend_check=[
                ("category", PivotRole.pivot_column),
                ("sales sum", PivotRole.pivot_measure),
                ("profit sum", PivotRole.pivot_measure),
                (MEASURE_NAME_TITLE, PivotRole.pivot_row),
                (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
            ],
        )

        check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=[],
            rows=["category"],
            measures=["sales sum", "profit sum"],
            simple_totals=PivotTotals(
                rows=[PivotTotals.item(level=0)],
                columns=[],
            ),
            custom_pivot_legend_check=[
                ("category", PivotRole.pivot_row),
                ("sales sum", PivotRole.pivot_measure),
                ("profit sum", PivotRole.pivot_measure),
                (MEASURE_NAME_TITLE, PivotRole.pivot_column),
                (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
            ],
        )

    def test_main_totals_with_annotation(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([sales])",
            },
        )

        pivot_abs = check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category"],
            rows=["region"],
            measures=["sales sum"],
            annotations=["profit sum"],
            simple_totals=PivotTotals(
                rows=[PivotTotals.item(level=0)],
                columns=[PivotTotals.item(level=0)],
            ),
        )
        col_titles = pivot_abs.get_flat_column_headers()
        row_titles = pivot_abs.get_flat_row_headers()
        assert col_titles == ["Furniture", "Office Supplies", "Technology", ""]
        assert row_titles == ["Central", "East", "South", "West", ""]

    def test_main_totals_with_multiple_measures(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([sales])",
            },
        )

        pivot_abs = check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category"],
            rows=["region", MEASURE_NAME_TITLE],
            measures=["sales sum", "profit sum"],
            simple_totals=PivotTotals(
                rows=[PivotTotals.item(level=0)],
                columns=[PivotTotals.item(level=0)],
            ),
        )
        col_titles = pivot_abs.get_flat_column_headers()
        row_compound_titles = pivot_abs.get_compound_row_headers()
        assert col_titles == ["Furniture", "Office Supplies", "Technology", ""]
        assert row_compound_titles == [
            ("Central", "sales sum"),
            ("Central", "profit sum"),
            ("East", "sales sum"),
            ("East", "profit sum"),
            ("South", "sales sum"),
            ("South", "profit sum"),
            ("West", "sales sum"),
            ("West", "profit sum"),
            ("", "sales sum"),
            ("", "profit sum"),
        ]

    def test_subtotals_with_multiple_measures(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([sales])",
            },
        )

        pivot_abs = check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category"],
            rows=[MEASURE_NAME_TITLE, "region"],
            measures=["sales sum", "profit sum"],
            simple_totals=PivotTotals(
                rows=[PivotTotals.item(level=1)],
                columns=[PivotTotals.item(level=0)],
            ),
        )
        col_titles = pivot_abs.get_flat_column_headers()
        row_compound_titles = pivot_abs.get_compound_row_headers()
        assert col_titles == ["Furniture", "Office Supplies", "Technology", ""]
        assert row_compound_titles == [
            ("sales sum", "Central"),
            ("sales sum", "East"),
            ("sales sum", "South"),
            ("sales sum", "West"),
            ("sales sum", ""),
            ("profit sum", "Central"),
            ("profit sum", "East"),
            ("profit sum", "South"),
            ("profit sum", "West"),
            ("profit sum", ""),
        ]

    def test_main_totals_with_only_mnames_one_one_side(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([sales])",
            },
        )

        # 1. Only Measure Names in rows
        pivot_abs = check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category"],
            rows=[MEASURE_NAME_TITLE],
            measures=["sales sum", "profit sum"],
            with_totals=True,
            check_totals=[()],  # override the autogenerated value because this is a corner case
        )
        col_titles = pivot_abs.get_flat_column_headers()
        row_titles = pivot_abs.get_flat_row_headers()
        assert col_titles == ["Furniture", "Office Supplies", "Technology", ""]
        assert row_titles == ["sales sum", "profit sum"]

        # 2. Only Measure Names in columns
        pivot_abs = check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=[MEASURE_NAME_TITLE],
            rows=["category"],
            measures=["sales sum", "profit sum"],
            with_totals=True,
            check_totals=[()],  # override the autogenerated value because this is a corner case
        )
        col_titles = pivot_abs.get_flat_column_headers()
        row_titles = pivot_abs.get_flat_row_headers()
        assert col_titles == ["sales sum", "profit sum"]
        assert row_titles == ["Furniture", "Office Supplies", "Technology", ""]
