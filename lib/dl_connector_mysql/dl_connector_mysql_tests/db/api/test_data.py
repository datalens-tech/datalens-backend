from datetime import datetime

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
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_mysql_tests.db.api.base import MySQLDataApiTestBase


class TestMySQLDataResult(MySQLDataApiTestBase, DefaultConnectorDataResultTestSuite):
    test_params = RegulatedTestParams(
        mark_features_skipped={
            DefaultConnectorDataResultTestSuite.array_support: "MySQL doesn't support arrays",
        }
    )

    def test_datetime_filter_with_zulu_timezone(
        self,
        saved_dataset: Dataset,
        control_api: SyncHttpDatasetApiV1,
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


class TestMySQLDataGroupBy(MySQLDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestMySQLDataRange(MySQLDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestMySQLDataDistinct(MySQLDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    pass


class TestMySQLDataPreview(MySQLDataApiTestBase, DefaultConnectorDataPreviewTestSuite):
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


class TestMySQLDataCache(MySQLDataApiTestBase, DefaultConnectorDataCacheTestSuite):
    data_caches_enabled = True
