from http import HTTPStatus

from dl_api_client.dsmaker.pivot_utils import get_pivot_response
from dl_api_client.dsmaker.primitives import (
    Dataset,
    PivotPagination,
    PivotTotals,
)
from dl_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import (
    FieldRole,
    PivotRole,
)
from dl_constants.internal_constants import MEASURE_NAME_TITLE
from dl_pivot.primitives import (
    PivotHeaderValue,
    PivotMeasureSorting,
    PivotMeasureSortingSettings,
)


class TestPivotErrors(DefaultApiTestBase):
    def test_multiple_measures_without_measure_name(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([sales])",
            },
        )

        pivot_resp = get_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category"],
            rows=["order_date"],
            measures=["sales sum", "profit sum"],
        )
        assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
        assert pivot_resp.bi_status_code == "ERR.DS_API.PIVOT.MEASURE_NAME.REQUIRED"

    def test_no_measures_with_measure_name(self, control_api, data_api, dataset_id):
        ds = control_api.load_dataset(dataset=Dataset(id=dataset_id)).dataset

        pivot_resp = get_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category"],
            rows=["order_date", MEASURE_NAME_TITLE],
            measures=[],
        )
        assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
        assert pivot_resp.bi_status_code == "ERR.DS_API.PIVOT.MEASURE_NAME.FORBIDDEN"

    def test_multiple_measures_with_double_measure_name(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([sales])",
            },
        )

        pivot_resp = get_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category", MEASURE_NAME_TITLE],
            rows=["order_date", MEASURE_NAME_TITLE],
            measures=["sales sum", "profit sum"],
        )
        assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
        assert pivot_resp.bi_status_code == "ERR.DS_API.PIVOT.MEASURE_NAME.DUPLICATE"

    def test_pagination_range(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
            },
        )

        def check_pagination_error(pivot_pagination: PivotPagination) -> None:
            pivot_resp = get_pivot_response(
                dataset=ds,
                data_api=data_api,
                columns=["category"],
                rows=["order_date"],
                measures=["sales sum"],
                pivot_pagination=pivot_pagination,
            )
            assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST

        check_pagination_error(PivotPagination(limit_rows=-9, offset_rows=8))
        check_pagination_error(PivotPagination(limit_rows=9, offset_rows=-8))
        check_pagination_error(PivotPagination(limit_rows=0, offset_rows=1))

    def test_measure_as_dimension(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([profit])",
            },
        )

        pivot_resp = get_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category"],
            rows=["sales sum"],
            measures=["profit sum"],
        )
        assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
        assert pivot_resp.bi_status_code == "ERR.DS_API.PIVOT.LEGEND.INVALID_ROLE"

    def test_dimension_as_measure(self, control_api, data_api, dataset_id):
        ds = control_api.load_dataset(dataset=Dataset(id=dataset_id)).dataset

        pivot_resp = get_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category"],
            rows=["city"],
            measures=["order_date"],
        )
        assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
        assert pivot_resp.bi_status_code == "ERR.DS_API.PIVOT.LEGEND.INVALID_ROLE"

    def test_uneven_data_columns(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
            },
        )

        pivot_resp = data_api.get_pivot(
            dataset=ds,
            fields=[
                ds.find_field(title="category").as_req_legend_item(legend_item_id=0, block_id=0),
                ds.find_field(title="sales sum").as_req_legend_item(legend_item_id=1, block_id=0),
                ds.find_field(title="order_date").as_req_legend_item(legend_item_id=2, block_id=1),
                ds.find_field(title="sales sum").as_req_legend_item(legend_item_id=3, role=FieldRole.total, block_id=1),
            ],
            pivot_structure=[
                ds.make_req_pivot_item(role=PivotRole.pivot_row, legend_item_ids=[0]),
                ds.make_req_pivot_item(role=PivotRole.pivot_column, legend_item_ids=[2]),
                ds.make_req_pivot_item(role=PivotRole.pivot_measure, legend_item_ids=[1, 3]),
            ],
            fail_ok=True,
        )
        assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
        assert pivot_resp.bi_status_code == "ERR.DS_API.PIVOT.UNEVEN_DATA_COLUMNS"

    def test_wrong_column_in_measure_sorting(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
            },
        )

        pivot_resp = get_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category"],
            rows=["order_date"],
            measures=["sales sum"],
            measures_sorting_settings=[
                PivotMeasureSorting(
                    column=PivotMeasureSortingSettings(header_values=[PivotHeaderValue(value="Not found")])
                )
            ],
        )
        assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
        assert pivot_resp.bi_status_code == "ERR.DS_API.PIVOT.SORTING.ROW_OR_COLUMN_NOT_FOUND"

    def test_wrong_role_spec_in_measure_sorting(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
            },
        )

        pivot_resp = get_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category"],
            rows=["order_date"],
            measures=["sales sum"],
            totals=[()],
            measures_sorting_settings=[
                PivotMeasureSorting(
                    column=PivotMeasureSortingSettings(
                        header_values=[PivotHeaderValue(value="")]
                    )  # sort by data, not totals!
                )
            ],
        )
        assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
        assert pivot_resp.bi_status_code == "ERR.DS_API.PIVOT.SORTING.ROW_OR_COLUMN_NOT_FOUND"

    def test_measure_sorting_multiple_sort_on_the_same_axis(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([sales])",
            },
        )
        sorting_settings = PivotMeasureSorting(
            column=PivotMeasureSortingSettings(
                header_values=[PivotHeaderValue(value="Furniture"), PivotHeaderValue(value="sales sum")]
            )
        )

        pivot_resp = get_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category", MEASURE_NAME_TITLE],
            rows=["order_date"],
            measures=["sales sum", "profit sum"],
            measures_sorting_settings=[sorting_settings, sorting_settings],
        )
        assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
        assert pivot_resp.bi_status_code == "ERR.DS_API.PIVOT.SORTING.MULTIPLE_COLUMNS_OR_ROWS"

    def test_measure_sorting_by_column_with_multiple_measures_in_rows(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([sales])",
            },
        )
        sorting_settings = PivotMeasureSorting(
            column=PivotMeasureSortingSettings(
                header_values=[PivotHeaderValue(value="Furniture"), PivotHeaderValue(value="sales sum")]
            )
        )

        pivot_resp = get_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["category"],
            rows=["order_date", MEASURE_NAME_TITLE],
            measures=["sales sum", "profit sum"],
            measures_sorting_settings=[sorting_settings, None],
        )
        assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
        assert pivot_resp.bi_status_code == "ERR.DS_API.PIVOT.SORTING.AGAINST_MULTIPLE_MEASURES"

    def test_measure_sorting_with_subtotals(self, control_api, data_api, dataset_id):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset_id=dataset_id,
            formulas={
                "sales sum": "SUM([sales])",
                "profit sum": "SUM([sales])",
            },
        )

        pivot_resp = get_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=[MEASURE_NAME_TITLE],
            rows=["category", "order_date"],
            measures=["sales sum", "profit sum"],
            measures_sorting_settings=[
                PivotMeasureSorting(
                    column=PivotMeasureSortingSettings(header_values=[PivotHeaderValue(value="sales sum")])
                ),
                None,
            ],
            simple_totals=PivotTotals(
                rows=[PivotTotals.item(level=1)],
            ),
        )
        assert pivot_resp.status_code == HTTPStatus.BAD_REQUEST
        assert pivot_resp.bi_status_code == "ERR.DS_API.PIVOT.SORTING.SUBTOTALS_ARE_NOT_ALLOWED"
