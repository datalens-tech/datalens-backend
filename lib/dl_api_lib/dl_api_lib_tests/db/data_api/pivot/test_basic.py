from http import HTTPStatus
from typing import Optional

from dl_api_client.dsmaker.pivot_utils import (
    check_pivot_response,
    get_pivot_response,
)
from dl_api_client.dsmaker.primitives import PivotPagination
from dl_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from dl_api_client.dsmaker.shortcuts.result_data import get_regular_result_data
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import OrderDirection
from dl_constants.internal_constants import MEASURE_NAME_TITLE
from dl_pivot.primitives import (
    PivotHeaderValue,
    PivotMeasureSorting,
    PivotMeasureSortingSettings,
)


class TestBasicPivot(DefaultApiTestBase):
    def test_basic_pivot(self, control_api, data_api, dataset_id):
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
            rows=["order_date"],
            measures=["sales sum"],
            min_col_cnt=3,
            min_row_cnt=100,
            min_value_cnt=100,
        )

    def test_pivot_multiple_measures(self, control_api, data_api, dataset_id):
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
            columns=["category", "sub_category"],
            rows=["order_date", MEASURE_NAME_TITLE, "city"],
            measures=["sales sum", "profit sum"],
            measures_sorting_settings=[
                PivotMeasureSorting(
                    row=PivotMeasureSortingSettings(
                        header_values=[
                            PivotHeaderValue(value="2014-01-04"),
                            PivotHeaderValue(value="sales sum"),
                            PivotHeaderValue(value="Naperville"),
                        ],
                    )
                ),
                None,
            ],
            min_col_cnt=10,
            min_row_cnt=100,
            min_value_cnt=100,
        )

        sorting_row_idx = None
        for row in pivot_abs.iter_rows():
            if row.get_compound_header() == ("2014-01-04", "sales sum", "Naperville"):
                sorting_row_idx = row.row_idx
                break
        assert sorting_row_idx is not None

        def _get_value(value: Optional[tuple]) -> float:
            if value is None:
                return float("-inf")
            return float(value[0][0])

        row_values = list(map(_get_value, pivot_abs.resp_data["pivot_data"]["rows"][sorting_row_idx]["values"]))
        assert sorted(row_values) == row_values

    def test_pivot_with_order_by(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([sales])",
            },
        )

        data_by_field = get_regular_result_data(
            ds,
            data_api,
            field_names=[
                "category",
                "sub_category",
                "order_date",
                "city",
                "sales sum",
                "profit sum",
            ],
        )

        pivot_resp = get_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category", "sub_category"],
            rows=["order_date", "city", MEASURE_NAME_TITLE],
            measures=["sales sum", "profit sum"],
            order_fields={"category": OrderDirection.desc, "order_date": OrderDirection.desc},
        )
        assert pivot_resp.status_code == HTTPStatus.OK
        result_data = pivot_resp.data

        # Check first column dimension
        category_values = []
        for col in result_data["pivot_data"]["columns"]:
            value = col[0][0][0]
            if value not in category_values:
                category_values.append(value)
        assert category_values == sorted(set(data_by_field["category"]), reverse=True)

        # Check first row dimension
        pivot_rows = result_data["pivot_data"]["rows"]
        date_values = []
        for row in pivot_rows:
            value = row["header"][0][0][0]
            if value not in date_values:
                date_values.append(value)
        assert date_values == sorted(set(data_by_field["order_date"]), reverse=True)

    def test_pivot_with_pagination(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([sales])",
            },
        )

        def get_pivot(pivot_pagination: Optional[PivotPagination] = None):
            pivot_resp = get_pivot_response(
                dataset=ds,
                data_api=data_api,
                columns=["category", "sub_category"],
                rows=["order_date", "city", MEASURE_NAME_TITLE],
                measures=["sales sum", "profit sum"],
                pivot_pagination=pivot_pagination,
            )
            assert pivot_resp.status_code == HTTPStatus.OK, pivot_resp.response_errors
            return pivot_resp.data

        # Save unpaginated table:
        result_data = get_pivot()
        initial_columns = result_data["pivot_data"]["columns"]
        initial_rows = result_data["pivot_data"]["rows"]

        # Get paginated table and compare
        result_data = get_pivot(pivot_pagination=PivotPagination(offset_rows=1, limit_rows=2))
        paginated_columns = result_data["pivot_data"]["columns"]
        paginated_rows = result_data["pivot_data"]["rows"]
        assert paginated_columns == initial_columns
        assert paginated_rows == initial_rows[1:3]

        # Pseudo-pagination
        result_data = get_pivot(pivot_pagination=PivotPagination(offset_rows=0, limit_rows=None))
        paginated_columns = result_data["pivot_data"]["columns"]
        paginated_rows = result_data["pivot_data"]["rows"]
        assert paginated_columns == initial_columns
        assert paginated_rows == initial_rows

    def test_pivot_with_remapped_titles(self, control_api, data_api, dataset_id):
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
            rows=["order_date"],
            measures=["sales sum"],
            title_mapping={
                "category": "My Dimension 1",
                "order_date": "My Dimension 2",
                "sales sum": "Measure",
            },
            min_col_cnt=3,
            min_row_cnt=100,
            min_value_cnt=100,
        )
