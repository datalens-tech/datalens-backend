from http import HTTPStatus

import pytest

from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import (
    CacheInvalidationMode,
    RawSQLLevel,
)


class TestCacheInvalidationLastResult(DefaultApiTestBase):
    """Tests for GET /api/data/v2/datasets/{ds_id}/cache_invalidation_last_result"""

    cache_invalidations_enabled = True
    data_caches_enabled = True
    raw_sql_level = RawSQLLevel.subselect

    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        from dl_api_lib_tests.db.config import CoreConnectionSettings

        return dict(
            db_name=CoreConnectionSettings.DB_NAME,
            host=CoreConnectionSettings.HOST,
            port=CoreConnectionSettings.PORT,
            username=CoreConnectionSettings.USERNAME,
            password=CoreConnectionSettings.PASSWORD,
            raw_sql_level=self.raw_sql_level.value,
            cache_invalidation_throttling_interval_sec=600,
        )

    def test_endpoint_returns_expected_schema(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        """Endpoint exists, returns 200 and response matches the expected schema."""
        update_data = dict(
            mode=CacheInvalidationMode.sql.value,
            sql="SELECT 'test_value'",
        )
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        resp = data_api.get_response_for_cache_invalidation_last_result(
            dataset_id=saved_dataset.id,
        )

        assert resp.status_code == HTTPStatus.OK, resp.json
        json_data = resp.json
        assert "status" in json_data
        assert "last_result" in json_data
        assert "timestamp" in json_data
        assert "last_result_error" in json_data
