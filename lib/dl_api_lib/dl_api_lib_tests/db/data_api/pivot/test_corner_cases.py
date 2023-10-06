from http import HTTPStatus
import json
from typing import Any

from dl_api_client.dsmaker.data_abstraction.pivot import PivotDataAbstraction
from dl_api_client.dsmaker.pivot_utils import (
    check_pivot_response,
    get_all_measure_cells,
)
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from dl_api_client.dsmaker.shortcuts.result_data import get_regular_result_data
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import (
    OrderDirection,
    PivotRole,
)
from dl_constants.internal_constants import (
    DIMENSION_NAME_TITLE,
    MEASURE_NAME_TITLE,
)


class TestPivotCornerCases(DefaultApiTestBase):
    def test_pivot_with_markup(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "category MU": "BOLD([category])",
                "order_date MU": "ITALIC(STR([order_date]))",
                "measure MU": 'BOLD(STR(SUM([sales]))) + " - " + ITALIC(STR(SUM([profit])))',
            },
        )

        data_by_field = get_regular_result_data(
            ds, data_api, field_names=["category MU", "order_date MU", "measure MU"]
        )

        category_liid = 0
        orderdate_liid = 1
        measure_liid = 2

        result_resp = data_api.get_pivot(
            dataset=ds,
            fields=[
                ds.find_field(title="category MU").as_req_legend_item(legend_item_id=category_liid),
                ds.find_field(title="order_date MU").as_req_legend_item(legend_item_id=orderdate_liid),
                ds.find_field(title="measure MU").as_req_legend_item(legend_item_id=measure_liid),
            ],
            pivot_structure=[
                ds.make_req_pivot_item(role=PivotRole.pivot_column, legend_item_ids=[category_liid]),
                ds.make_req_pivot_item(role=PivotRole.pivot_row, legend_item_ids=[orderdate_liid]),
                ds.make_req_pivot_item(role=PivotRole.pivot_measure, legend_item_ids=[measure_liid]),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK
        result_data = result_resp.data

        # Check legend
        legend_tuples = [(item.title, item.role_spec.role) for item in result_data["pivot"]["structure"]]
        assert legend_tuples == [
            ("category MU", PivotRole.pivot_column),
            ("order_date MU", PivotRole.pivot_row),
            ("measure MU", PivotRole.pivot_measure),
            (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
        ]

        def dumps(value: Any) -> str:
            return json.dumps(value, sort_keys=True)

        # Check columns
        category_values = {dumps(col[0][0][0]) for col in result_data["pivot_data"]["columns"]}
        assert category_values == {
            dumps({"type": "bold", "content": {"type": "text", "content": "Office Supplies"}}),
            dumps({"type": "bold", "content": {"type": "text", "content": "Furniture"}}),
            dumps({"type": "bold", "content": {"type": "text", "content": "Technology"}}),
        }

        # Check row headers
        pivot_rows = result_data["pivot_data"]["rows"]
        date_values = {dumps(row["header"][0][0][0]) for row in pivot_rows}
        assert date_values.issuperset(
            {
                dumps({"type": "italics", "content": {"type": "text", "content": "2014-01-03"}}),
                dumps({"type": "italics", "content": {"type": "text", "content": "2014-01-06"}}),
                dumps({"type": "italics", "content": {"type": "text", "content": "2014-01-09"}}),
            }
        )

        # Check measure values
        assert len(pivot_rows) > 100
        all_measure_cells = get_all_measure_cells(pivot_rows)
        measure_values = sorted({dumps(cell[0][0]) for cell in all_measure_cells})
        original_measure_values = sorted({dumps(val) for val in data_by_field["measure MU"]})
        assert measure_values == original_measure_values

    def test_pivot_no_measures(self, control_api, data_api, dataset_id):
        ds = control_api.load_dataset(dataset=Dataset(id=dataset_id)).dataset

        check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category", "sub_category"],
            rows=["order_date", "city"],
            measures=[],
            min_col_cnt=10,
            min_row_cnt=100,
            max_value_cnt=0,
        )

    def test_pivot_no_dimensions_multiple_measures(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([sales])",
            },
        )

        check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=[],
            rows=[],
            measures=["sales sum", "profit sum"],
            min_col_cnt=2,
            max_col_cnt=2,
            min_row_cnt=1,
            max_row_cnt=1,
            min_value_cnt=2,
            max_value_cnt=2,
            custom_pivot_legend_check=[
                ("sales sum", PivotRole.pivot_measure),
                ("profit sum", PivotRole.pivot_measure),
                (MEASURE_NAME_TITLE, PivotRole.pivot_column),
                (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
            ],
        )

    def test_pivot_only_row_dimensions_one_measure(self, control_api, data_api, dataset_id):
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
            columns=[],
            rows=["category", "order_date"],
            measures=["sales sum"],
            min_col_cnt=1,
            max_col_cnt=1,
            custom_pivot_legend_check=[
                ("category", PivotRole.pivot_row),
                ("order_date", PivotRole.pivot_row),
                ("sales sum", PivotRole.pivot_measure),
                (MEASURE_NAME_TITLE, PivotRole.pivot_column),
                (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
            ],
        )

    def test_pivot_only_column_dimensions_multiple_measures_no_mnames(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([sales])",
            },
        )

        check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category", "order_date"],
            rows=[],
            measures=["sales sum", "profit sum"],
            min_row_cnt=2,
            max_row_cnt=2,
            custom_pivot_legend_check=[
                ("category", PivotRole.pivot_column),
                ("order_date", PivotRole.pivot_column),
                ("sales sum", PivotRole.pivot_measure),
                ("profit sum", PivotRole.pivot_measure),
                (MEASURE_NAME_TITLE, PivotRole.pivot_row),
                (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
            ],
        )

    def test_pivot_only_column_dimensions_multiple_measures_with_mnames(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([sales])",
            },
        )

        check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category", "order_date", MEASURE_NAME_TITLE],
            rows=[],
            measures=["sales sum", "profit sum"],
            min_row_cnt=1,
            max_row_cnt=1,
        )

    def test_single_measure_with_duplicate_measure_name(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
            },
        )

        category_liid = 0
        orderdate_liid = 1
        sales_liid = 2
        mname_liid = 3

        def get_pivot(duplicate_measure_name: bool) -> dict:
            result_resp = data_api.get_pivot(
                dataset=ds,
                fields=[
                    ds.find_field(title="category").as_req_legend_item(legend_item_id=category_liid),
                    ds.find_field(title="order_date").as_req_legend_item(legend_item_id=orderdate_liid),
                    ds.find_field(title="sales sum").as_req_legend_item(legend_item_id=sales_liid),
                    ds.measure_name_as_req_legend_item(legend_item_id=mname_liid),
                ],
                pivot_structure=[
                    ds.make_req_pivot_item(role=PivotRole.pivot_column, legend_item_ids=[category_liid]),
                    ds.make_req_pivot_item(role=PivotRole.pivot_column, legend_item_ids=[mname_liid]),
                    ds.make_req_pivot_item(role=PivotRole.pivot_row, legend_item_ids=[orderdate_liid]),
                    *(
                        (ds.make_req_pivot_item(role=PivotRole.pivot_row, legend_item_ids=[mname_liid]),)
                        if duplicate_measure_name
                        else ()
                    ),
                    ds.make_req_pivot_item(role=PivotRole.pivot_measure, legend_item_ids=[sales_liid]),
                ],
                fail_ok=True,
            )
            assert result_resp.status_code == HTTPStatus.OK
            return result_resp.data

        single_result_data = get_pivot(duplicate_measure_name=False)
        double_result_data = get_pivot(duplicate_measure_name=True)

        # Check legend
        legend_tuples = [(item.title, item.role_spec.role) for item in double_result_data["pivot"]["structure"]]
        assert legend_tuples == [
            ("category", PivotRole.pivot_column),
            ("Measure Names", PivotRole.pivot_column),
            ("order_date", PivotRole.pivot_row),
            ("Measure Names", PivotRole.pivot_row),
            ("sales sum", PivotRole.pivot_measure),
            (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
        ]

        # Check column headers
        for col in double_result_data["pivot_data"]["columns"]:
            assert len(col) == 2

        # Check row headers
        for row in double_result_data["pivot_data"]["rows"]:
            assert len(row["header"]) == 2

        assert len(double_result_data["pivot_data"]["rows"]) == len(single_result_data["pivot_data"]["rows"])
        for single_row, double_row in zip(
            single_result_data["pivot_data"]["rows"], double_result_data["pivot_data"]["rows"]
        ):
            assert len(single_row["values"]) == len(double_row["values"])
            for single_cell, double_cell in zip(single_row["values"], double_row["values"]):
                if single_cell is None or double_cell is None:
                    assert single_cell == double_cell
                else:
                    assert len(single_cell) == len(double_cell) == 1
                    assert single_cell[0][0:2] == double_cell[0][0:2]  # exclude pivot_item_id (idx=2) from comparison

    def test_pivot_only_row_dimensions_no_measures(self, control_api, data_api, dataset_id):
        ds = control_api.load_dataset(dataset=Dataset(id=dataset_id)).dataset

        check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=[],
            rows=["category", "order_date"],
            measures=[],
            max_col_cnt=1,  # There will be 1 column without any headers or values
            min_row_cnt=100,
            max_value_cnt=0,
        )

    def test_pivot_only_single_column_dimension_no_measures(self, control_api, data_api, dataset_id):
        ds = control_api.load_dataset(dataset=Dataset(id=dataset_id)).dataset

        check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["order_date"],
            rows=[],
            measures=[],
            max_row_cnt=1,  # There will be 1 row without any headers or values
            min_col_cnt=100,
            max_value_cnt=0,
        )

    def test_pivot_duplicate_measures(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([sales])",
            },
        )

        check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category", "sub_category"],
            rows=["order_date", "city", MEASURE_NAME_TITLE],
            measures=["sales sum", "profit sum", "sales sum"],
            min_col_cnt=10,
            min_row_cnt=100,
            min_value_cnt=100,
        )

    def test_pivot_empty_string_dimension_values(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "empty city": 'IF [city] = "New York" THEN "" ELSE [city] END',
            },
        )

        check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["empty city"],
            rows=["category"],
            measures=["sales sum"],
            order_fields={"empty city": OrderDirection.desc},
            with_totals=True,
        )

    def test_pivot_null_dimension_values(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "null city": 'IF [city] = "New York" THEN NULL ELSE [city] END',
            },
        )

        check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["null city"],
            rows=["category"],
            measures=["sales sum"],
            with_totals=True,
        )

    def test_pivot_sorting_with_totals(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
            },
        )

        def get_pivot(direction: OrderDirection) -> PivotDataAbstraction:
            return check_pivot_response(
                dataset=ds,
                data_api=data_api,
                columns=["category"],
                rows=["region"],
                measures=["sales sum"],
                order_fields={"category": direction},
                with_totals=True,
            )

        pivot_abs = get_pivot(OrderDirection.asc)
        col_titles = pivot_abs.get_flat_column_headers()
        assert col_titles == ["Furniture", "Office Supplies", "Technology", ""]

        pivot_abs = get_pivot(OrderDirection.desc)
        col_titles = pivot_abs.get_flat_column_headers()
        assert col_titles == ["Technology", "Office Supplies", "Furniture", ""]
