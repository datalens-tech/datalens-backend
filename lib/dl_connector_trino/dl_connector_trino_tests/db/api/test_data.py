from datetime import datetime

import pytest

from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataCacheTestSuite,
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataPreviewTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)
from dl_api_lib_testing.data_api_base import DataApiTestParams
from dl_constants.enums import WhereClauseOperation
from dl_core_testing.database import Db

from dl_connector_trino_tests.db.api.base import TrinoDataApiTestBase


class TestTrinoDataResult(TrinoDataApiTestBase, DefaultConnectorDataResultTestSuite):
    def test_datetime_filter_with_zulu_timezone(
        self,
        saved_dataset: Dataset,
        data_api_test_params: DataApiTestParams,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        ds = saved_dataset
        ds.result_schema["Datetime field"] = ds.field(formula=f"DATETIME([{data_api_test_params.date_field}])")
        result_resp = self.get_result(ds, data_api, field_names=("Datetime field",))
        assert result_resp.status_code == 200, result_resp.json
        values = [datetime.fromisoformat(row[0]) for row in get_data_rows(result_resp)]
        bounds = [min(values).isoformat() + "Z", max(values).isoformat() + "Z"]

        filtered_result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="Datetime field"),
            ],
            filters=[
                ds.find_field(title="Datetime field").filter(WhereClauseOperation.BETWEEN, bounds),
            ],
            fail_ok=True,
        )
        assert filtered_result_resp.status_code == 200, filtered_result_resp.json
        rows = get_data_rows(filtered_result_resp)
        assert len(rows) == len(values)  # no filtration should occur

    @pytest.mark.parametrize(
        "field_title,filter_field_title,is_numeric",
        (
            ("array_int_value", "int_value", True),
            ("array_str_value", "str_value", False),
            ("array_str_value", "concat_const", False),
            pytest.param(
                "array_str_value",
                "concat_field",
                False,
                marks=pytest.mark.xfail(reason="BI-6239"),
            ),
            ("array_float_value", "float_value", True),
            ("array_float_value", "none_value", False),
        ),
    )
    def test_array_contains_field(
        self,
        request: pytest.FixtureRequest,
        db: Db,
        sample_table_schema: str | None,
        saved_connection_id: str,
        dataset_params: dict,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        field_title: str,
        filter_field_title: str,
        is_numeric: bool,
    ) -> None:
        super().test_array_contains_field(
            request=request,
            db=db,
            sample_table_schema=sample_table_schema,
            saved_connection_id=saved_connection_id,
            dataset_params=dataset_params,
            control_api=control_api,
            data_api=data_api,
            field_title=field_title,
            filter_field_title=filter_field_title,
            is_numeric=is_numeric,
        )

    @pytest.mark.xfail(reason="BI-6239")
    def test_get_result_with_formula_in_where(
        self, saved_dataset: Dataset, data_api_test_params: DataApiTestParams, data_api: SyncHttpDataApiV2
    ) -> None:
        super().test_get_result_with_formula_in_where(
            saved_dataset=saved_dataset,
            data_api_test_params=data_api_test_params,
            data_api=data_api,
        )


class TestTrinoDataGroupBy(TrinoDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestTrinoDataRange(TrinoDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestTrinoDataDistinct(TrinoDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    pass


class TestTrinoDataPreview(TrinoDataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    def test_percent_char(
        self,
        saved_dataset: Dataset,
        control_api: SyncHttpDatasetApiV1,
        data_api_test_params: DataApiTestParams,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        ds = saved_dataset
        ds.result_schema["Test"] = ds.field(formula=f'CONCAT([{data_api_test_params.two_dims[0]}], "%")')
        ds = control_api.apply_updates(dataset=ds).dataset

        preview_resp = data_api.get_preview(dataset=ds)
        assert preview_resp.status_code == 200
        preview_data = preview_resp.data
        assert all(row["data"][0].count("%") == 1 for row in preview_data["result_data"][0]["rows"])


class TestTrinoDataCache(TrinoDataApiTestBase, DefaultConnectorDataCacheTestSuite):
    data_caches_enabled = True
