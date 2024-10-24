from http import HTTPStatus

import pytest

from dl_api_client.dsmaker.pivot_utils import (
    check_pivot_response,
    get_all_measure_cells,
)
from dl_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from dl_api_client.dsmaker.shortcuts.result_data import get_regular_result_data
from dl_api_lib_testing.api_base import DefaultApiTestBase
from dl_constants.enums import PivotRole
from dl_constants.internal_constants import (
    DIMENSION_NAME_TITLE,
    MEASURE_NAME_TITLE,
)


class TestPivotWithAnnotations(DefaultApiTestBase):
    def test_pivot_multiple_measures_with_annotation(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([profit])",
                "order count": "COUNT([sales])",
            },
        )

        check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category", "sub_category"],
            rows=["order_date", "city", MEASURE_NAME_TITLE],
            measures=["sales sum", "profit sum"],
            annotations=["order count"],
            min_col_cnt=10,
            min_row_cnt=100,
            min_value_cnt=100,
        )

    def test_pivot_multiple_measures_with_targeted_annotation(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([profit])",
                "order count": "COUNT([sales])",
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
                "order count",
            ],
        )
        category_leg_id = 0
        subcategory_leg_id = 1
        orderdate_leg_id = 2
        city_leg_id = 3
        mname_leg_id = 4
        profit_leg_id = 5
        sales_leg_id = 6
        anno_leg_id = 7

        result_resp = data_api.get_pivot(
            dataset=ds,
            fields=[
                ds.find_field(title="category").as_req_legend_item(legend_item_id=category_leg_id),
                ds.find_field(title="sub_category").as_req_legend_item(legend_item_id=subcategory_leg_id),
                ds.find_field(title="order_date").as_req_legend_item(legend_item_id=orderdate_leg_id),
                ds.find_field(title="city").as_req_legend_item(legend_item_id=city_leg_id),
                ds.measure_name_as_req_legend_item(legend_item_id=mname_leg_id),
                ds.find_field(title="sales sum").as_req_legend_item(legend_item_id=sales_leg_id),
                ds.find_field(title="profit sum").as_req_legend_item(legend_item_id=profit_leg_id),
                ds.find_field(title="order count").as_req_legend_item(legend_item_id=anno_leg_id),
            ],
            pivot_structure=[
                ds.make_req_pivot_item(role=PivotRole.pivot_column, legend_item_ids=[category_leg_id]),
                ds.make_req_pivot_item(role=PivotRole.pivot_column, legend_item_ids=[subcategory_leg_id]),
                ds.make_req_pivot_item(role=PivotRole.pivot_row, legend_item_ids=[orderdate_leg_id]),
                ds.make_req_pivot_item(role=PivotRole.pivot_row, legend_item_ids=[city_leg_id]),
                ds.make_req_pivot_item(role=PivotRole.pivot_row, legend_item_ids=[mname_leg_id]),
                ds.make_req_pivot_item(role=PivotRole.pivot_measure, legend_item_ids=[sales_leg_id]),
                ds.make_req_pivot_item(role=PivotRole.pivot_measure, legend_item_ids=[profit_leg_id]),
                ds.make_req_pivot_item(
                    role=PivotRole.pivot_annotation,
                    legend_item_ids=[anno_leg_id],
                    annotation_type="color",
                    target_legend_item_ids=[profit_leg_id],
                ),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK
        result_data = result_resp.data

        # Check legend
        legend_tuples = [(item.title, item.role_spec.role) for item in result_data["pivot"]["structure"]]
        assert legend_tuples == [
            ("category", PivotRole.pivot_column),
            ("sub_category", PivotRole.pivot_column),
            ("order_date", PivotRole.pivot_row),
            ("city", PivotRole.pivot_row),
            (MEASURE_NAME_TITLE, PivotRole.pivot_row),
            ("sales sum", PivotRole.pivot_measure),
            ("profit sum", PivotRole.pivot_measure),
            ("order count", PivotRole.pivot_annotation),
            (DIMENSION_NAME_TITLE, PivotRole.pivot_info),
        ]

        # Check measure values
        pivot_rows = result_data["pivot_data"]["rows"]
        # Check annotation values
        all_measure_cells = get_all_measure_cells(pivot_rows)
        sales_annotation_values = sorted(
            {float(cell[1][0]) for cell in all_measure_cells if cell[0][1] == sales_leg_id and len(cell) > 1}
        )
        profit_annotation_values = sorted(
            {float(cell[1][0]) for cell in all_measure_cells if cell[0][1] == profit_leg_id and len(cell) > 1}
        )
        original_annotation_values = sorted({float(val) for val in data_by_field["order count"]})
        assert sales_annotation_values == []
        assert profit_annotation_values == pytest.approx(original_annotation_values)

    def test_pivot_multi_measures_with_annotation_same_as_one_measure(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "Count": "COUNT()",
            },
        )

        check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category", "sub_category"],
            rows=["order_date", "city", MEASURE_NAME_TITLE],
            measures=["sales sum", "Count"],
            annotations=["sales sum"],
            min_col_cnt=10,
            min_row_cnt=100,
            min_value_cnt=100,
        )

    def test_pivot_with_multiple_annotations(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([profit])",
                "order count": "COUNT([sales])",
            },
        )

        check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category"],
            rows=["order_date"],
            measures=["sales sum"],
            annotations=["profit sum", "order count"],
        )

    def test_pivot_with_annotation_by_dimension(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "order count": "COUNT([sales])",
            },
        )

        check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category"],
            rows=["order_date"],
            measures=["sales sum"],
            annotations=["order_date"],
        )
