from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.pivot_utils import check_pivot_response
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from dl_api_lib_testing.connector.complex_queries import DefaultBasicComplexQueryTestSuite

from dl_connector_trino_tests.db.api.base import TrinoDataApiTestBase


class TestTrinoBasicComplexQueries(TrinoDataApiTestBase, DefaultBasicComplexQueryTestSuite):
    def make_pivot_request(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
    ) -> None:
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "X": "-[sales] % 3",
                "Y": "[sales] % 2",
                "value": "MAX(COUNTD([sales]) WITHIN [Y])",
            },
        )
        check_pivot_response(
            dataset=ds,
            data_api=data_api,
            columns=["Y"],
            rows=["X"],
            measures=["value"],
        )

    def test_duplicate_parametrized_expression_in_select(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
    ) -> None:
        self.make_pivot_request(
            control_api=control_api,
            data_api=data_api,
            saved_dataset=saved_dataset,
        )
