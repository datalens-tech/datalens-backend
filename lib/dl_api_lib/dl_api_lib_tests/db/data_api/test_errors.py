import pytest

from dl_api_lib_testing.data_api_base import DataApiTestParams
from dl_api_lib_testing.api_base import DefaultApiTestBase
from dl_constants.enums import WhereClauseOperation


class TestDataApiErrors(DefaultApiTestBase):
    @pytest.fixture(scope="function")
    def data_api_test_params(self, sample_table) -> DataApiTestParams:
        # This default is defined for the sample table
        return DataApiTestParams(
            two_dims=("category", "city"),
            summable_field="sales",
            range_field="sales",
            distinct_field="city",
            date_field="order_date",
        )

    def test_distinct_measure_filter_error(self, saved_dataset, data_api, data_api_test_params):
        ds = saved_dataset
        ds.result_schema["Measure"] = ds.field(formula=f"SUM([{data_api_test_params.summable_field}])")

        distinct_resp = data_api.get_distinct(
            dataset=ds,
            field=ds.find_field(title=data_api_test_params.distinct_field),
            filters=[
                ds.find_field(title="Measure").filter(WhereClauseOperation.GT, [100]),
            ],
            fail_ok=True,
        )
        assert distinct_resp.status_code == 400
        assert distinct_resp.bi_status_code == "ERR.DS_API.FILTER.MEASURE_UNSUPPORTED"

    def test_range_measure_filter_error(self, saved_dataset, data_api, data_api_test_params):
        ds = saved_dataset
        ds.result_schema["Measure"] = ds.field(formula=f"SUM([{data_api_test_params.summable_field}])")

        range_resp = data_api.get_value_range(
            dataset=ds,
            field=ds.find_field(title=data_api_test_params.range_field),
            filters=[
                ds.find_field(title="Measure").filter(WhereClauseOperation.GT, [100]),
            ],
            fail_ok=True,
        )
        assert range_resp.status_code == 400
        assert range_resp.bi_status_code == "ERR.DS_API.FILTER.MEASURE_UNSUPPORTED"
