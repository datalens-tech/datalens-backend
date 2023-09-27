from typing import Optional

import pytest

from dl_api_client.dsmaker.pivot_utils import check_pivot_response
from dl_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from dl_api_lib.pivot.primitives import (
    PivotHeaderRoleSpec,
    PivotHeaderValue,
    PivotMeasureSorting,
    PivotMeasureSortingSettings,
)
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import (
    NotificationType,
    OrderDirection,
    PivotHeaderRole,
    WhereClauseOperation,
)
from dl_constants.internal_constants import MEASURE_NAME_TITLE


class TestPivotWithTotals(DefaultApiTestBase):
    def test_basic_pivot_with_grand_total(self, control_api, data_api, dataset_id):
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
            totals=[()],
        )
        col_titles = pivot_abs.get_flat_column_headers()
        row_titles = pivot_abs.get_flat_row_headers()
        assert col_titles == ["Furniture", "Office Supplies", "Technology", ""]
        assert row_titles == ["Central", "East", "South", "West", ""]

    def test_pivot_empty_data_with_grand_totals(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([profit])",
            },
        )

        pivot_abs = check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category", MEASURE_NAME_TITLE],
            rows=[],
            measures=["sales sum", "profit sum"],
            measures_sorting_settings=[
                PivotMeasureSorting(
                    column=PivotMeasureSortingSettings(
                        header_values=[PivotHeaderValue(value=""), PivotHeaderValue(value="sales sum")],
                        direction=OrderDirection.desc,
                        role_spec=PivotHeaderRoleSpec(role=PivotHeaderRole.total),
                    )
                ),
                None,
            ],
            totals=[()],
            filters=[ds.find_field("category").filter(WhereClauseOperation.STARTSWITH, ["lol"])],
        )

        assert pivot_abs.get_compound_column_headers() == [("", "sales sum"), ("", "profit sum")]
        assert pivot_abs.get_compound_row_headers() == [()]
        assert pivot_abs.get_1d_mapper().get_value_count() == 2  # no values except grand totals

    @pytest.mark.parametrize("role", [PivotHeaderRole.total, PivotHeaderRole.data])
    def test_pivot_measure_sorting_with_total_and_empty_name_column(self, control_api, data_api, dataset_id, role):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "category without Furniture": "IF([category] = 'Furniture', '', [category])",
            },
        )

        pivot_abs = check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category without Furniture"],
            rows=["region"],
            measures=["sales sum"],
            measures_sorting_settings=[
                PivotMeasureSorting(
                    column=PivotMeasureSortingSettings(
                        header_values=[PivotHeaderValue(value="")],
                        direction=OrderDirection.desc,
                        role_spec=PivotHeaderRoleSpec(role=role),
                    )
                )
            ],
            totals=[()],
        )
        col_titles = pivot_abs.get_flat_column_headers()
        assert col_titles == ["", "Office Supplies", "Technology", ""]  # first '' is from formula, second is from total

        def _get_value(value: Optional[tuple]) -> float:
            if value is None:
                return float("-inf")
            return float(value[0][0])

        column_idx = 0 if role == PivotHeaderRole.data else -1
        total_values = [_get_value(row["values"][column_idx]) for row in pivot_abs.resp_data["pivot_data"]["rows"]]
        last_value = total_values.pop()
        # pivot is sorted by values of '' column with selected role,
        # except for the last value, which is from totals row
        assert sorted(total_values, reverse=True) == total_values
        if role == PivotHeaderRole.total:  # total column: the last value is a grand total => is not None:
            assert last_value != float("-inf")
        else:  # data column: the last value is a column total => is None
            assert last_value == float("-inf")

    def test_basic_pivot_with_grand_total_and_simple_subtotals(self, control_api, data_api, dataset_id):
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
            totals=[
                ("category",),  # region total by category
                ("region",),  # category total by region
                (),  # Grand Total
            ],
        )
        col_titles = pivot_abs.get_flat_column_headers()
        row_titles = pivot_abs.get_flat_row_headers()
        assert col_titles == ["Furniture", "Office Supplies", "Technology", ""]
        assert row_titles == ["Central", "East", "South", "West", ""]

    def test_pivot_with_totals_and_measure_filters(self, control_api, data_api, dataset_id):
        """
        Check that if measure filters are added to the request,
        then totals are disabled.
        """

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
            totals=[("category",), ("region",), ()],
            filters=[ds.find_field("sales sum").filter(WhereClauseOperation.GT, [10.0])],
            expected_notifications=[NotificationType.totals_removed_due_to_measure_filter],
        )
        col_titles = pivot_abs.get_flat_column_headers()
        row_titles = pivot_abs.get_flat_row_headers()
        assert col_titles == ["Furniture", "Office Supplies", "Technology"]  # No totals
        assert row_titles == ["Central", "East", "South", "West"]  # No totals
