import pytest

from dl_api_client.dsmaker.api.data_api import (
    HttpDataApiResponse,
    SyncHttpDataApiV2,
)
from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_lib_testing.helpers.data_source import data_source_settings_from_table
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_api_lib_tests.db.config import CoreConnectionSettings
from dl_constants.enums import (
    CacheInvalidationMode,
    RawSQLLevel,
)
from dl_core_testing.database import (
    Db,
    make_table,
)


class CacheInvalidationDataFlowBase(DefaultApiTestBase):
    cache_invalidations_enabled = True
    data_caches_enabled = True
    raw_sql_level = RawSQLLevel.subselect

    @pytest.fixture(scope="class")
    def connection_params(self) -> dict:
        return dict(
            db_name=CoreConnectionSettings.DB_NAME,
            host=CoreConnectionSettings.HOST,
            port=CoreConnectionSettings.PORT,
            username=CoreConnectionSettings.USERNAME,
            password=CoreConnectionSettings.PASSWORD,
            raw_sql_level=self.raw_sql_level.value,
            cache_ttl_sec=86400,
            cache_invalidation_throttling_interval_sec=600,
        )


class TestCacheInvalidationSqlModeDataFlow(CacheInvalidationDataFlowBase):
    def test_result_with_sql_invalidation_returns_data(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        update_data = dict(
            mode=CacheInvalidationMode.sql.value,
            sql="SELECT 'v1'",
        )
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        result_resp = data_api.get_result(
            dataset=saved_dataset,
            fields=[
                saved_dataset.find_field(title="category"),
            ],
        )
        assert result_resp.status_code == 200, result_resp.response_errors
        data = get_data_rows(response=result_resp)
        assert len(data) > 0

    def test_sql_payload_affects_cache_key(
        self,
        db: Db,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_connection_id: str,
    ) -> None:
        db_table = make_table(db)
        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id, **data_source_settings_from_table(db_table)
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        ds.result_schema["measure"] = ds.field(formula="SUM([int_value])")
        ds = control_api.apply_updates(dataset=ds).dataset
        ds = control_api.save_dataset(ds).dataset

        # Step 1: Set SQL invalidation
        update_data = dict(
            mode=CacheInvalidationMode.sql.value,
            sql="SELECT 'v1'",
        )
        ds = control_api.update_cache_invalidation(ds, update_data).dataset
        ds = control_api.save_dataset(ds).dataset

        # Step 2: First query — populates cache
        def get_data(fail_ok: bool = False) -> HttpDataApiResponse:
            result_resp = data_api.get_result(
                dataset=ds,
                fields=[
                    ds.find_field(title="int_value"),
                    ds.find_field(title="measure"),
                ],
                fail_ok=fail_ok,
            )
            return result_resp

        # we need to get the data twice to get rid of stale inval cache payload
        resp_v1 = get_data()
        assert resp_v1.status_code == 200, resp_v1.response_errors
        data_v1 = get_data_rows(response=resp_v1)
        assert len(data_v1) > 0

        resp_v1 = get_data()
        assert resp_v1.status_code == 200, resp_v1.response_errors
        data_v1 = get_data_rows(response=resp_v1)
        assert len(data_v1) > 0

        # Drop the table
        db_table.db.drop_table(db_table.table)
        assert not db_table.db.has_table(db_table.table.name), "Table should have been dropped"
        # Same invalidation payload → cache hit
        resp_cached = get_data()
        assert resp_cached.status_code == 200, resp_cached.response_errors
        data_cached = get_data_rows(response=resp_cached)
        assert data_cached == data_v1, "Cache should return same data with same invalidation payload"
        # Change invalidation payload → cache miss
        update_data_v2 = dict(
            mode=CacheInvalidationMode.sql.value,
            sql="SELECT 'v2'",
        )
        ds = control_api.update_cache_invalidation(ds, update_data_v2).dataset
        ds = control_api.save_dataset(ds).dataset

        # Different payload → cache miss → DB query fails (table dropped). Still need to get data twice
        resp_stale = get_data()
        assert resp_stale.status_code == 200, resp_v1.response_errors
        data_v1 = get_data_rows(response=resp_stale)
        assert len(data_v1) > 0

        resp_miss = get_data(fail_ok=True)
        assert (
            resp_miss.status_code != 200
        ), "Should fail because table is dropped and cache key changed due to different invalidation payload"

    def test_graceful_degradation_invalid_sql(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        update_data = dict(
            mode=CacheInvalidationMode.sql.value,
            sql="SELECT * FROM nonexistent_table_xyz_12345",
        )
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        result_resp = data_api.get_result(
            dataset=saved_dataset,
            fields=[
                saved_dataset.find_field(title="category"),
            ],
        )
        # The main query should still succeed even if invalidation query fails
        assert result_resp.status_code == 200, result_resp.response_errors
        data = get_data_rows(response=result_resp)
        assert len(data) > 0


class TestCacheInvalidationFormulaModeDataFlow(CacheInvalidationDataFlowBase):
    def test_result_with_formula_invalidation_returns_data(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        update_data = dict(
            mode=CacheInvalidationMode.formula.value,
            field=dict(
                guid="cache_inval_field",
                title="inval_field",
                formula="STR([category])",
            ),
        )
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        result_resp = data_api.get_result(
            dataset=saved_dataset,
            fields=[
                saved_dataset.find_field(title="category"),
            ],
        )
        assert result_resp.status_code == 200, result_resp.response_errors
        data = get_data_rows(response=result_resp)
        assert len(data) > 0


class TestCacheInvalidationModeOffDataFlow(CacheInvalidationDataFlowBase):
    def test_mode_off_result_works(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        # Explicitly set mode=off
        update_data = dict(
            mode=CacheInvalidationMode.off.value,
        )
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        result_resp = data_api.get_result(
            dataset=saved_dataset,
            fields=[
                saved_dataset.find_field(title="category"),
            ],
        )
        assert result_resp.status_code == 200, result_resp.response_errors
        data = get_data_rows(response=result_resp)
        assert len(data) > 0

    def test_mode_off_cache_works_after_table_drop(
        self,
        db: Db,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_connection_id: str,
    ) -> None:
        db_table = make_table(db)
        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id, **data_source_settings_from_table(db_table)
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        ds.result_schema["measure"] = ds.field(formula="SUM([int_value])")
        ds = control_api.apply_updates(dataset=ds).dataset
        ds = control_api.save_dataset(ds).dataset

        # mode=off (default)
        def get_data() -> HttpDataApiResponse:
            result_resp = data_api.get_result(
                dataset=ds,
                fields=[
                    ds.find_field(title="int_value"),
                    ds.find_field(title="measure"),
                ],
            )
            return result_resp

        resp1 = get_data()
        assert resp1.status_code == 200, resp1.response_errors
        data1 = get_data_rows(response=resp1)

        # Drop table — cache should still work
        db_table.db.drop_table(db_table.table)

        resp2 = get_data()
        assert resp2.status_code == 200, resp2.response_errors
        data2 = get_data_rows(response=resp2)
        assert data2 == data1, "Cache should return same data when mode=off"
