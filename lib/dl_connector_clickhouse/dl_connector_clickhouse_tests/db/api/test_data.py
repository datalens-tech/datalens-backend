from datetime import (
    datetime,
    timedelta,
)

from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
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

from dl_connector_clickhouse_tests.db.api.base import (
    ClickHouseDataApiReadonlyUserTestBase,
    ClickHouseDataApiTestBase,
)


class TestClickHouseDataResult(ClickHouseDataApiTestBase, DefaultConnectorDataResultTestSuite):
    def test_datetrunc_with_fixed(
        self, saved_dataset: Dataset, data_api_test_params: DataApiTestParams, data_api: SyncHttpDataApiV2
    ):
        ds = saved_dataset
        ds.result_schema["Countd"] = ds.field(formula=f"COUNTD([{data_api_test_params.distinct_field}] FIXED)")
        ds.result_schema["Datetrunc"] = ds.field(
            formula=f"DATETRUNC(DATETIME([{data_api_test_params.date_field}]), 'day', 1)"
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title="Countd")],
            filters=[
                ds.find_field(title="Datetrunc").filter(
                    WhereClauseOperation.BETWEEN,
                    [datetime.today() - timedelta(days=1), datetime.today()],
                ),
            ],
        )
        data_rows = get_data_rows(result_resp)
        assert data_rows


class TestClickHouseDataGroupBy(ClickHouseDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestClickHouseDataRange(ClickHouseDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestClickHouseDataDistinct(ClickHouseDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    pass


class TestClickHouseDataPreview(ClickHouseDataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass


class TestClickHouseDataCache(ClickHouseDataApiTestBase, DefaultConnectorDataCacheTestSuite):
    data_caches_enabled = True


class TestClickHouseReadonlyUserDataResult(ClickHouseDataApiReadonlyUserTestBase, DefaultConnectorDataResultTestSuite):
    pass
