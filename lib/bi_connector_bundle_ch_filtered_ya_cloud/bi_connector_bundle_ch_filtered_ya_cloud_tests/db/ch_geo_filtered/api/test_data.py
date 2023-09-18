from dl_api_client.dsmaker.primitives import Dataset
from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_lib_testing.data_api_base import DataApiTestParams, StandardizedDataApiTestBase

from bi_connector_bundle_ch_filtered_ya_cloud_tests.db.ch_geo_filtered.api.base import (
    CHGeoFilteredDataApiTestBase, CHGeoFilteredDownloadableDataApiTestBase,
)


class TestCHGeoFilteredResult(CHGeoFilteredDataApiTestBase, StandardizedDataApiTestBase):
    def test_data_export(
            self, saved_dataset: Dataset,
            data_api: SyncHttpDataApiV2,
            data_api_test_params: DataApiTestParams,
    ) -> None:
        result_resp = self.get_result(saved_dataset, data_api, field_names=(data_api_test_params.two_dims[0],))
        assert result_resp.json['data_export_forbidden']


class TestCHGeoFilteredDownloadableResult(CHGeoFilteredDownloadableDataApiTestBase, StandardizedDataApiTestBase):
    def test_data_export(
            self, saved_dataset: Dataset,
            data_api: SyncHttpDataApiV2,
            data_api_test_params: DataApiTestParams,
    ) -> None:
        result_resp = self.get_result(saved_dataset, data_api, field_names=(data_api_test_params.two_dims[0],))
        assert not result_resp.json['data_export_forbidden']
