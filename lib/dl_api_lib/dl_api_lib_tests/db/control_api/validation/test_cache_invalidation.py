import pytest

from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import (
    CacheInvalidationMode,
    WhereClauseOperation,
)


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

        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
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

        saved_dataset = control_api.update_cache_invalidation(saved_dataset, data=update_data).dataset
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

        resp = control_api.update_cache_invalidation(saved_dataset, update_data)
        ds = control_api.save_dataset(resp.dataset).dataset
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

        resp = control_api.update_cache_invalidation(saved_dataset, update_data)
        ds = control_api.save_dataset(resp.dataset).dataset
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

        resp = control_api.update_cache_invalidation(saved_dataset, update_data)
        ds = control_api.save_dataset(resp.dataset).dataset
        assert resp.status_code == 200
        assert ds.cache_invalidation_source.cache_invalidation_error

    def test_validation_formula_mode_with_empty_formula(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """Test that mode=formula requires formula to be non-empty"""
        update_data = dict(
            mode=CacheInvalidationMode.formula.value,
            field=dict(
                guid="cache_field_1",
                formula="",  # empty formula
            ),
        )

        resp = control_api.update_cache_invalidation(saved_dataset, update_data)
        ds = control_api.save_dataset(resp.dataset).dataset
        assert resp.status_code == 200
        assert ds.cache_invalidation_source.cache_invalidation_error
        assert (
            ds.cache_invalidation_source.cache_invalidation_error.locator == "cache_invalidation_source.field.formula"
        )

    def test_validation_formula_mode_with_whitespace_formula(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """Test that mode=formula requires formula to be non-whitespace"""
        update_data = dict(
            mode=CacheInvalidationMode.formula.value,
            field=dict(
                guid="cache_field_1",
                formula="   ",  # whitespace only formula
            ),
        )

        resp = control_api.update_cache_invalidation(saved_dataset, update_data)
        ds = control_api.save_dataset(resp.dataset).dataset
        assert resp.status_code == 200
        assert ds.cache_invalidation_source.cache_invalidation_error
        assert (
            ds.cache_invalidation_source.cache_invalidation_error.locator == "cache_invalidation_source.field.formula"
        )

    def test_validation_formula_mode_with_invalid_syntax(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """Test that mode=formula validates formula syntax"""
        update_data = dict(
            mode=CacheInvalidationMode.formula.value,
            field=dict(
                guid="cache_field_1",
                formula="MAX([field] +",  # invalid syntax - unclosed parenthesis
            ),
        )

        resp = control_api.update_cache_invalidation(saved_dataset, update_data)
        ds = control_api.save_dataset(resp.dataset).dataset
        assert resp.status_code == 200
        assert ds.cache_invalidation_source.cache_invalidation_error
        assert (
            ds.cache_invalidation_source.cache_invalidation_error.locator == "cache_invalidation_source.field.formula"
        )
        assert "Syntax" in ds.cache_invalidation_source.cache_invalidation_error.title

    def test_validation_formula_mode_with_unknown_field(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """Test that mode=formula validates field references"""
        update_data = dict(
            mode=CacheInvalidationMode.formula.value,
            field=dict(
                guid="cache_field_1",
                formula="MAX([nonexistent_field_12345])",  # unknown field
            ),
        )

        resp = control_api.update_cache_invalidation(saved_dataset, update_data)
        ds = control_api.save_dataset(resp.dataset).dataset
        assert resp.status_code == 200
        assert ds.cache_invalidation_source.cache_invalidation_error
        assert (
            ds.cache_invalidation_source.cache_invalidation_error.locator == "cache_invalidation_source.field.formula"
        )
        assert (
            "Unknown field found in formula: nonexistent_field_12345"
            in ds.cache_invalidation_source.cache_invalidation_error.message
        )

    def test_validation_formula_mode_with_valid_formula(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """Test that valid formula passes validation (must return string)"""
        # Use a string field from the dataset (category is a string field)
        # Formula must return string type for cache invalidation
        update_data = dict(
            mode=CacheInvalidationMode.formula.value,
            field=dict(
                guid="cache_field_1",
                formula="MAX([category])",  # MAX on string returns string
            ),
        )

        resp = control_api.update_cache_invalidation(saved_dataset, update_data)
        ds = control_api.save_dataset(resp.dataset).dataset
        assert resp.status_code == 200
        assert ds.cache_invalidation_source.cache_invalidation_error is None
        assert ds.cache_invalidation_source.mode == CacheInvalidationMode.formula

    def test_validation_formula_mode_with_non_string_result_type(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """Test that formula returning non-string type fails validation"""
        # Use a numeric field - SUM returns integer/float, not string
        update_data = dict(
            mode=CacheInvalidationMode.formula.value,
            field=dict(
                guid="cache_field_1",
                formula="SUM([quantity])",  # SUM returns integer, not string
            ),
        )

        resp = control_api.update_cache_invalidation(saved_dataset, update_data)
        ds = control_api.save_dataset(resp.dataset).dataset
        assert resp.status_code == 200
        assert ds.cache_invalidation_source.cache_invalidation_error is not None
        assert (
            ds.cache_invalidation_source.cache_invalidation_error.locator == "cache_invalidation_source.field.formula"
        )
        assert "Result Type" in ds.cache_invalidation_source.cache_invalidation_error.title
        assert "string" in ds.cache_invalidation_source.cache_invalidation_error.message.lower()

    def test_validation_formula_mode_with_date_result_type(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """Test that formula returning date type fails validation"""
        # Use a date field - MAX on date returns date, not string
        update_data = dict(
            mode=CacheInvalidationMode.formula.value,
            field=dict(
                guid="cache_field_1",
                formula="MAX([order_date])",  # MAX on date returns date, not string
            ),
        )

        resp = control_api.update_cache_invalidation(saved_dataset, update_data)
        ds = control_api.save_dataset(resp.dataset).dataset
        assert resp.status_code == 200
        assert ds.cache_invalidation_source.cache_invalidation_error is not None
        assert (
            ds.cache_invalidation_source.cache_invalidation_error.locator == "cache_invalidation_source.field.formula"
        )
        assert "Result Type" in ds.cache_invalidation_source.cache_invalidation_error.title

    def test_validation_formula_mode_with_str_cast(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """Test that formula with STR() cast passes validation"""
        # Use STR() to convert numeric result to string
        update_data = dict(
            mode=CacheInvalidationMode.formula.value,
            field=dict(
                guid="cache_field_1",
                formula="STR(MAX([quantity]))",  # STR converts to string
            ),
        )

        resp = control_api.update_cache_invalidation(saved_dataset, update_data)
        ds = control_api.save_dataset(resp.dataset).dataset
        assert resp.status_code == 200
        assert ds.cache_invalidation_source.cache_invalidation_error is None
        assert ds.cache_invalidation_source.mode == CacheInvalidationMode.formula

    def test_validation_formula_mode_with_concat_formula(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """Test that CONCAT formula passes SQL translation validation"""
        update_data = dict(
            mode=CacheInvalidationMode.formula.value,
            field=dict(
                guid="cache_field_1",
                formula="CONCAT(MAX([category]), ' - ', STR(SUM([quantity])))",
            ),
        )

        resp = control_api.update_cache_invalidation(saved_dataset, update_data)
        ds = control_api.save_dataset(resp.dataset).dataset
        assert resp.status_code == 200
        assert ds.cache_invalidation_source.cache_invalidation_error is None
        assert ds.cache_invalidation_source.mode == CacheInvalidationMode.formula

    def test_validation_formula_mode_with_filter_unknown_field(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """Test that filter referencing unknown field fails validation"""
        update_data = dict(
            mode=CacheInvalidationMode.formula.value,
            field=dict(
                guid="cache_field_1",
                formula="MAX([category])",
            ),
            filters=[
                dict(
                    id="filter_1",
                    field_guid="nonexistent_field_guid_12345",
                    default_filters=[],
                ),
            ],
        )

        resp = control_api.update_cache_invalidation(saved_dataset, update_data)
        ds = control_api.save_dataset(resp.dataset).dataset
        assert resp.status_code == 200
        assert ds.cache_invalidation_source.cache_invalidation_error is not None
        assert ds.cache_invalidation_source.cache_invalidation_error.locator == "cache_invalidation_source.filters"
        assert "Invalid Filter" in ds.cache_invalidation_source.cache_invalidation_error.title
        assert "nonexistent_field_guid_12345" in ds.cache_invalidation_source.cache_invalidation_error.message

    def test_validation_formula_mode_with_filter_incompatible_operation(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """Test that filter with incompatible operation for field type fails validation"""
        # Get the guid of a date field (order_date) - STARTSWITH is not allowed for dates
        date_field = saved_dataset.find_field(title="order_date")

        update_data = dict(
            mode=CacheInvalidationMode.formula.value,
            field=dict(
                guid="cache_field_1",
                formula="MAX([category])",
            ),
            filters=[
                dict(
                    id="filter_1",
                    field_guid=date_field.id,
                    default_filters=[
                        dict(
                            column=date_field.id,
                            operation=WhereClauseOperation.STARTSWITH.name,
                            values=["2023"],
                        ),
                    ],
                ),
            ],
        )

        resp = control_api.update_cache_invalidation(saved_dataset, update_data)
        ds = control_api.save_dataset(resp.dataset).dataset
        assert resp.status_code == 200
        assert ds.cache_invalidation_source.cache_invalidation_error is not None
        assert ds.cache_invalidation_source.cache_invalidation_error.locator == "cache_invalidation_source.filters"
        assert "Invalid Filter Operation" in ds.cache_invalidation_source.cache_invalidation_error.title

    def test_validation_formula_mode_with_valid_filter(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """Test that valid filter passes validation"""
        # Get the guid of a string field (category) - EQ is allowed for strings
        category_field = saved_dataset.find_field(title="category")

        update_data = dict(
            mode=CacheInvalidationMode.formula.value,
            field=dict(
                guid="cache_field_1",
                formula="MAX([category])",
            ),
            filters=[
                dict(
                    id="filter_1",
                    field_guid=category_field.id,
                    default_filters=[
                        dict(
                            column=category_field.id,
                            operation=WhereClauseOperation.EQ.name,
                            values=["Furniture"],
                        ),
                    ],
                ),
            ],
        )

        resp = control_api.update_cache_invalidation(saved_dataset, update_data)
        ds = control_api.save_dataset(resp.dataset).dataset
        assert resp.status_code == 200
        assert ds.cache_invalidation_source.cache_invalidation_error is None
        assert ds.cache_invalidation_source.mode == CacheInvalidationMode.formula

    def test_validation_formula_mode_with_multiple_valid_filters(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """Test that multiple valid filters pass validation"""
        category_field = saved_dataset.find_field(title="category")
        city_field = saved_dataset.find_field(title="city")

        update_data = dict(
            mode=CacheInvalidationMode.formula.value,
            field=dict(
                guid="cache_field_1",
                formula="MAX([category])",
            ),
            filters=[
                dict(
                    id="filter_1",
                    field_guid=category_field.id,
                    default_filters=[
                        dict(
                            column=category_field.id,
                            operation=WhereClauseOperation.EQ.name,
                            values=["Furniture"],
                        ),
                    ],
                ),
                dict(
                    id="filter_2",
                    field_guid=city_field.id,
                    default_filters=[
                        dict(
                            column=city_field.id,
                            operation=WhereClauseOperation.CONTAINS.name,
                            values=["New"],
                        ),
                    ],
                ),
            ],
        )

        resp = control_api.update_cache_invalidation(saved_dataset, update_data)
        ds = control_api.save_dataset(resp.dataset).dataset
        assert resp.status_code == 200
        assert ds.cache_invalidation_source.cache_invalidation_error is None
        assert ds.cache_invalidation_source.mode == CacheInvalidationMode.formula

    def test_change_mode_without_save_preserves_old_fields(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """Test that changing mode without saving the dataset preserves fields from the old mode
        and adds fields for the new mode.

        Scenario: set sql mode with a query, save, then switch to formula mode
        without saving. The response should contain both the old sql field
        and the new formula-related fields.
        """
        sql_query = "SELECT MAX(updated_at) FROM my_table"

        # Step 1: Set sql mode and save
        update_data_sql = dict(
            mode=CacheInvalidationMode.sql.value,
            sql=sql_query,
        )
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, data=update_data_sql).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        assert saved_dataset.cache_invalidation_source.mode == CacheInvalidationMode.sql
        assert saved_dataset.cache_invalidation_source.sql == sql_query

        # Step 2: Switch to formula mode WITHOUT saving
        update_data_formula = dict(
            mode=CacheInvalidationMode.formula.value,
            field=dict(
                guid="cache_field_1",
                formula="MAX([category])",
            ),
        )
        resp = control_api.update_cache_invalidation(saved_dataset, data=update_data_formula)
        ds = resp.dataset
        assert resp.status_code == 200

        # Step 3: Verify new mode is set
        assert ds.cache_invalidation_source.mode == CacheInvalidationMode.formula

        # The sql field from the old mode should be preserved (not cleared)
        assert ds.cache_invalidation_source.sql == sql_query

        # The formula field for the new mode should be present
        assert ds.cache_invalidation_source.field is not None
        assert ds.cache_invalidation_source.field.formula == "MAX([category])"

    def test_change_mode_without_save_preserves_formula_fields_when_switching_to_sql(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        """Test that switching from formula to sql mode without saving preserves
        formula-related fields and adds sql field.

        Scenario: set formula mode with a formula and filters, save, then switch
        to sql mode without saving. The response should contain both the old
        formula/filter fields and the new sql field.
        """
        category_field = saved_dataset.find_field(title="category")

        # Step 1: Set formula mode with field and filter, then save
        update_data_formula = dict(
            mode=CacheInvalidationMode.formula.value,
            field=dict(
                guid="cache_field_1",
                formula="MAX([category])",
            ),
            filters=[
                dict(
                    id="filter_1",
                    field_guid=category_field.id,
                    default_filters=[
                        dict(
                            column=category_field.id,
                            operation=WhereClauseOperation.EQ.name,
                            values=["Furniture"],
                        ),
                    ],
                ),
            ],
        )
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, data=update_data_formula).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        assert saved_dataset.cache_invalidation_source.mode == CacheInvalidationMode.formula
        assert saved_dataset.cache_invalidation_source.field is not None
        assert len(saved_dataset.cache_invalidation_source.filters) == 1

        # Step 2: Switch to sql mode WITHOUT saving
        sql_query = "SELECT MAX(updated_at) FROM my_table"
        update_data_sql = dict(
            mode=CacheInvalidationMode.sql.value,
            sql=sql_query,
        )
        resp = control_api.update_cache_invalidation(saved_dataset, data=update_data_sql)
        ds = resp.dataset
        assert resp.status_code == 200

        # Step 3: Verify new mode is set
        assert ds.cache_invalidation_source.mode == CacheInvalidationMode.sql

        # The sql field for the new mode should be present
        assert ds.cache_invalidation_source.sql == sql_query

        # The formula-related fields from the old mode should be preserved
        assert ds.cache_invalidation_source.field is not None
        assert ds.cache_invalidation_source.field.formula == "MAX([category])"
        assert len(ds.cache_invalidation_source.filters) == 1
