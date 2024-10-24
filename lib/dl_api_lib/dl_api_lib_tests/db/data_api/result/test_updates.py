from __future__ import annotations

import uuid

import pytest

from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_lib_testing.api_base import DefaultApiTestBase
from dl_api_lib_testing.data_api_base import DataApiTestParams
from dl_constants.enums import FieldType


class TestUpdates(DefaultApiTestBase):
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

    def test_result_with_updates(self, saved_dataset, data_api, data_api_test_params):
        ds = saved_dataset
        id_1, id_2, id_3 = (str(uuid.uuid4()) for _ in range(3))
        result_resp = data_api.get_result(
            dataset=ds,
            updates=[
                ds.field(
                    id=id_1,
                    title="First",
                    formula=f"SUM([{data_api_test_params.summable_field}]) / 100",
                    type=FieldType.MEASURE,
                ).add(),
                # use an invalid field type for the second one to make sure it fixes itself
                ds.field(
                    id=id_2,
                    title="Second",
                    formula=f"COUNTD([{data_api_test_params.distinct_field}])",
                    type=FieldType.DIMENSION,
                ).add(),
                ds.field(id=id_3, title="Third", formula="[First] / [Second]", type=FieldType.MEASURE).add(),
            ],
            fields=[
                ds.field(id=id_1),
                ds.field(id=id_2),
                ds.field(id=id_3),
            ],
        )
        assert result_resp.status_code == 200, result_resp.response_errors
        result_data = result_resp.data
        titles = [field.title for field in result_data["fields"]]
        assert titles == ["First", "Second", "Third"]

    def test_get_result_add_update_field_without_avatar(self, saved_dataset, data_api):
        ds = saved_dataset
        ds.result_schema["Some Field"] = ds.field(formula="NOW()")

        result_resp = data_api.get_result(
            dataset=ds,
            updates=[
                ds.result_schema["Some Field"].update(calc_mode="direct"),
            ],
            fields=[
                ds.find_field(title="Some Field"),
            ],
        )
        assert result_resp.status_code == 200, result_resp.response_errors
        data_rows = get_data_rows(result_resp)
        assert data_rows
