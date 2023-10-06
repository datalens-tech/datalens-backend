from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_lib_testing.connector.data_api_suites import (
    DefaultConnectorDataDistinctTestSuite,
    DefaultConnectorDataGroupByFormulaTestSuite,
    DefaultConnectorDataPreviewTestSuite,
    DefaultConnectorDataRangeTestSuite,
    DefaultConnectorDataResultTestSuite,
)
from dl_constants.enums import WhereClauseOperation
from dl_testing.regulated_test import RegulatedTestParams

from dl_connector_bitrix_gds_tests.ext.api.base import (
    BitrixDataApiTestBase,
    BitrixSmartTablesDataApiTestBase,
)


class TestBitrixDataResult(BitrixDataApiTestBase, DefaultConnectorDataResultTestSuite):
    test_params = RegulatedTestParams(
        mark_features_skipped={
            DefaultConnectorDataResultTestSuite.array_support: "Bitrix doesn't support arrays",
        }
    )


class TestBitrixDataGroupBy(BitrixDataApiTestBase, DefaultConnectorDataGroupByFormulaTestSuite):
    pass


class TestBitrixDataRange(BitrixDataApiTestBase, DefaultConnectorDataRangeTestSuite):
    pass


class TestBitrixDataDistinct(BitrixDataApiTestBase, DefaultConnectorDataDistinctTestSuite):
    test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultConnectorDataDistinctTestSuite.test_date_filter_distinct: "Can't create a new table in bitrix tests",
        }
    )


class TestBitrixDataPreview(BitrixDataApiTestBase, DefaultConnectorDataPreviewTestSuite):
    pass


class TestBitrixSmartTablesData(BitrixSmartTablesDataApiTestBase):
    def test_bitrix_string_to_date(
        self,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        ds = saved_dataset
        ds.result_schema["Date from string user_field"] = ds.field(formula=f"DATE([UF_CRM_5_1694020695771])")
        self.get_preview(ds, data_api)

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title="Date from string user_field")],
            filters=[
                ds.find_field(title="Date from string user_field").filter(
                    op=WhereClauseOperation.BETWEEN,
                    values=["2023-09-05", "2023-09-06"],
                )
            ],
        )
        assert result_resp.status_code == 200, result_resp.json
        assert get_data_rows(result_resp)
