from datetime import (
    datetime,
    timedelta,
)
import decimal

from clickhouse_sqlalchemy import types as ch_types
import pytest

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
from dl_constants.enums import (
    UserDataType,
    WhereClauseOperation,
)
from dl_core_testing.database import (
    C,
    Db,
    DbTable,
    make_table,
)

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


class TestClickHouseDecimalResult(ClickHouseDataApiTestBase):
    @pytest.fixture(scope="class")
    def sample_table(self, db: Db) -> DbTable:
        return make_table(
            db=db,
            columns=[
                C("decimal_value", UserDataType.float, sa_type=ch_types.Decimal(10, 2)),
            ],
            data=[
                {"decimal_value": decimal.Decimal("123.45")},
                {"decimal_value": decimal.Decimal("0")},
                {"decimal_value": decimal.Decimal("-99.99")},
            ],
        )

    def test_decimal_in_result(self, saved_dataset: Dataset, data_api: SyncHttpDataApiV2) -> None:
        ds = saved_dataset
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title="decimal_value")],
        )
        data_rows = get_data_rows(result_resp)
        values = [row[0] for row in data_rows]

        assert values == ["0.0", "123.45", "-99.99"]
