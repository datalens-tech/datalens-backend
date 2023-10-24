import datetime

from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)
from dl_api_lib_testing.data_api_base import DataApiTestParams
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_metrica_tests.ext.api.base import MetricaDataApiTestBase


class TestMetricaDataResult(MetricaDataApiTestBase, DefaultConnectorDataResultTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultConnectorDataResultTestSuite.test_basic_result: "Metrica doesn't support SUM",
            DefaultConnectorDataResultTestSuite.test_duplicated_expressions: "Metrica doesn't support SUM",
            DefaultConnectorDataResultTestSuite.test_dates: "Metrica doesn't support DATE",
            DefaultConnectorDataResultTestSuite.test_get_result_with_string_filter_operations_for_numbers: "Metrica doesn't support ICONTAINS",
        },
        mark_features_skipped={
            DefaultConnectorDataResultTestSuite.array_support: "Metrica doesn't support arrays",
        },
    )

    def test_metrica_result(
        self,
        saved_dataset: Dataset,
        data_api_test_params: DataApiTestParams,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        ds = saved_dataset
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="Дата просмотра"),
                ds.find_field(title="Просмотров в минуту"),
            ],
            filters=[
                ds.find_field(title="Дата просмотра").filter("BETWEEN", values=["2019-12-01", "2019-12-07 12:00:00"])
            ],
            order_by=[ds.find_field(title="Просмотров в минуту")],
            limit=3,
        )
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) == 3


class TestMetricaDataGroupBy(MetricaDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultConnectorDataGroupByFormulaTestSuite.test_complex_result: "Metrica doesn't support LEN"
        }
    )


class TestMetricaDataRange(MetricaDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    def test_basic_range(
        self,
        saved_dataset: Dataset,
        data_api_test_params: DataApiTestParams,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        ds = saved_dataset

        range_resp = self.get_range(ds, data_api, field_name=data_api_test_params.range_field)
        range_rows = get_data_rows(range_resp)
        min_val, max_val = map(datetime.datetime.fromisoformat, range_rows[0])
        assert min_val <= max_val
        assert max_val - min_val < datetime.timedelta(seconds=1)


class TestMetricaDataDistinct(MetricaDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultConnectorDataDistinctTestSuite.test_date_filter_distinct: "Metrica doesn't support ICONTAINS"
        }
    )


# preview is not available for Metrica
