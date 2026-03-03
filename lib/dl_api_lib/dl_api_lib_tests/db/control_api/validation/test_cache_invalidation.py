import pytest

from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import (
    Dataset,
)
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import CacheInvalidationMode


class TestCacheInvalidation(DefaultApiTestBase):
    @pytest.mark.parametrize(
        "mode",
        [CacheInvalidationMode.sql, CacheInvalidationMode.off],
    )
    def test_change_cache_invalidation_mode(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        mode: CacheInvalidationMode,
    ) -> None:
        sql = "SELECT 1" if mode == CacheInvalidationMode.sql else None
        update_data = dict(mode=mode.value, sql=sql)

        saved_dataset = control_api.update_cache_invalidation(
            saved_dataset, saved_dataset.cache_invalidation_source, update_data
        ).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        assert saved_dataset.cache_invalidation_source.mode == mode, "Cache invalidation mode should be updated"

        saved_dataset = control_api.get_dataset(saved_dataset.id).dataset
        assert saved_dataset.cache_invalidation_source.mode == mode, "Cache invalidation mode should be saved"

    def test_change_cache_invalidation_sql(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        sql_query = "SELECT MAX(updated_at) FROM my_table"
        update_data = dict(
            mode=CacheInvalidationMode.sql.value,
            sql=sql_query,
        )

        saved_dataset = control_api.update_cache_invalidation(
            saved_dataset, saved_dataset.cache_invalidation_source, data=update_data
        ).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        assert (
            saved_dataset.cache_invalidation_source.mode == CacheInvalidationMode.sql
        ), "Cache invalidation mode should be sql"
        assert saved_dataset.cache_invalidation_source.sql == sql_query, "Cache invalidation sql should be updated"

        saved_dataset = control_api.get_dataset(saved_dataset.id).dataset
        assert saved_dataset.cache_invalidation_source.sql == sql_query, "Cache invalidation sql should be saved"

    def test_validation_sql_mode_requires_sql_field(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """Test that mode=sql requires sql field to be filled"""
        update_data = dict(
            mode=CacheInvalidationMode.sql.value,
            # sql field is missing (None by default)
        )

        resp = control_api.update_cache_invalidation(
            saved_dataset, saved_dataset.cache_invalidation_source, update_data
        )
        ds = resp.dataset
        assert resp.status_code == 200
        assert ds.cache_invalidation_source.cache_invalidation_error

    def test_validation_sql_mode_with_empty_sql(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """Test that mode=sql requires sql field to be non-empty"""
        update_data = dict(
            mode=CacheInvalidationMode.sql.value,
            sql="",  # empty sql
        )

        resp = control_api.update_cache_invalidation(
            saved_dataset, saved_dataset.cache_invalidation_source, update_data
        )
        ds = resp.dataset
        assert resp.status_code == 200
        assert ds.cache_invalidation_source.cache_invalidation_error

    def test_validation_formula_mode_requires_field(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """Test that mode=formula requires field to be filled"""
        update_data = dict(
            mode=CacheInvalidationMode.formula.value,
            # field is missing (None by default)
        )

        resp = control_api.update_cache_invalidation(
            saved_dataset, saved_dataset.cache_invalidation_source, update_data
        )
        ds = resp.dataset
        assert resp.status_code == 200
        assert ds.cache_invalidation_source.cache_invalidation_error
