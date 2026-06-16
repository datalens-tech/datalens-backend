from __future__ import annotations

from collections.abc import Callable
from http import HTTPStatus

import pytest

from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
import dl_api_lib.app.data_api.resources.dataset.cache_invalidation_test as cache_invalidation_test_module
from dl_api_lib.enums import USPermissionKind
from dl_api_lib.utils.base import check_permission_on_entry
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants import (
    CacheInvalidationMode,
    RawSQLLevel,
    WhereClauseOperation,
)
from dl_core.us_connection_base import ConnectionBase
from dl_core.us_entry import USEntry

PermissionPatch = Callable[[USEntry, USPermissionKind], bool]


def _connection_read_denied_patch() -> PermissionPatch:
    """Force connection-level `read` permission to False; delegate other checks."""
    real = check_permission_on_entry

    def patched(entry: USEntry, permission: USPermissionKind) -> bool:
        if isinstance(entry, ConnectionBase) and permission is USPermissionKind.read:
            return False
        return real(entry, permission)

    return patched


class CacheInvalidationTestBase(DefaultApiTestBase):
    """Base class for cache invalidation test endpoint tests."""

    def _call_cache_invalidation_test(
        self,
        data_api: SyncHttpDataApiV2,
        dataset_id: str,
        body: dict | None = None,
    ) -> dict:
        """Helper to call the cache invalidation test endpoint and return raw response."""
        response = data_api.get_response_for_cache_invalidation_test(
            dataset_id=dataset_id,
            raw_body=body,
        )
        return {
            "status_code": response.status_code,
            "json": response.json,
        }


class TestCacheInvalidationTestSqlMode(CacheInvalidationTestBase):
    """Tests for POST /api/data/v2/datasets/{ds_id}/cache_invalidation_test (SQL mode)"""

    raw_sql_level = RawSQLLevel.subselect

    def test_success_sql_mode(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        """Test successful cache invalidation test with SQL mode returning a string."""
        # Set up cache invalidation in SQL mode with a string-returning query
        update_data = {
            "mode": CacheInvalidationMode.sql.value,
            "sql": "SELECT 'hello'",
        }
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        # Call the cache invalidation test endpoint
        resp = self._call_cache_invalidation_test(
            data_api=data_api,
            dataset_id=saved_dataset.id,
        )

        assert resp["status_code"] == HTTPStatus.OK, resp["json"]
        assert "result" in resp["json"]
        assert "value" in resp["json"]["result"]
        assert "query" in resp["json"]["result"]
        assert resp["json"]["result"]["query"] == "SELECT 'hello'"
        assert resp["json"]["result"]["value"] == "hello"

    def test_success_sql_mode_cast_to_string(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        """Test successful cache invalidation test with SQL mode using toString() cast."""
        update_data = {
            "mode": CacheInvalidationMode.sql.value,
            "sql": "SELECT toString(42)",
        }
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        resp = self._call_cache_invalidation_test(
            data_api=data_api,
            dataset_id=saved_dataset.id,
        )

        assert resp["status_code"] == HTTPStatus.OK, resp["json"]
        assert resp["json"]["result"]["value"] == "42"

    def test_error_non_string_result(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        """Test that a query returning a non-string type (integer) returns 400."""
        update_data = {
            "mode": CacheInvalidationMode.sql.value,
            "sql": "SELECT 1",
        }
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        resp = self._call_cache_invalidation_test(
            data_api=data_api,
            dataset_id=saved_dataset.id,
        )

        assert resp["status_code"] == HTTPStatus.BAD_REQUEST, resp["json"]
        assert "CACHE_INVALIDATION_TEST" in resp["json"].get("code", "")
        assert "NON_STRING_RESULT" in resp["json"].get("code", "")

    def test_error_mode_off(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        """Test that mode=off returns 400."""
        # Ensure cache invalidation is off (default)
        update_data = {"mode": CacheInvalidationMode.off.value}
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        resp = self._call_cache_invalidation_test(
            data_api=data_api,
            dataset_id=saved_dataset.id,
        )

        assert resp["status_code"] == HTTPStatus.BAD_REQUEST, resp["json"]
        assert "CACHE_INVALIDATION_TEST" in resp["json"].get("code", "")
        assert "MODE_OFF" in resp["json"].get("code", "")

    def test_error_multiple_rows(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        """Test that a query returning multiple rows returns 400."""
        update_data = {
            "mode": CacheInvalidationMode.sql.value,
            "sql": "SELECT toString(number) FROM system.numbers LIMIT 5",
        }
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        resp = self._call_cache_invalidation_test(
            data_api=data_api,
            dataset_id=saved_dataset.id,
        )

        assert resp["status_code"] == HTTPStatus.BAD_REQUEST, resp["json"]
        assert "CACHE_INVALIDATION_TEST" in resp["json"].get("code", "")
        assert "INVALID_RESULT" in resp["json"].get("code", "")

    def test_error_value_too_long(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        """Test that a result value exceeding 100 characters returns 400."""
        # Generate a string longer than 100 characters
        update_data = {
            "mode": CacheInvalidationMode.sql.value,
            "sql": "SELECT repeat('a', 200)",
        }
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        resp = self._call_cache_invalidation_test(
            data_api=data_api,
            dataset_id=saved_dataset.id,
        )

        assert resp["status_code"] == HTTPStatus.BAD_REQUEST, resp["json"]
        assert "CACHE_INVALIDATION_TEST" in resp["json"].get("code", "")
        assert "INVALID_RESULT" in resp["json"].get("code", "")

    def test_error_query_execution_failure(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        """Test that a failing SQL query returns 400."""
        update_data = {
            "mode": CacheInvalidationMode.sql.value,
            "sql": "SELECT * FROM nonexistent_table_xyz_12345",
        }
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        resp = self._call_cache_invalidation_test(
            data_api=data_api,
            dataset_id=saved_dataset.id,
        )

        assert resp["status_code"] == HTTPStatus.BAD_REQUEST, resp["json"]
        assert "CACHE_INVALIDATION_TEST" in resp["json"].get("code", "")


class TestCacheInvalidationTestSqlModeSubselectNotAllowed(CacheInvalidationTestBase):
    """Test that SQL mode fails when connection does not support subselect."""

    raw_sql_level = RawSQLLevel.off

    def test_error_subselect_not_allowed(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        """Test that SQL mode with raw_sql_level=off returns 400 (subselect not allowed)."""
        update_data = {
            "mode": CacheInvalidationMode.sql.value,
            "sql": "SELECT 1",
        }
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        resp = self._call_cache_invalidation_test(
            data_api=data_api,
            dataset_id=saved_dataset.id,
        )

        assert resp["status_code"] == HTTPStatus.BAD_REQUEST, resp["json"]
        assert "CACHE_INVALIDATION_TEST" in resp["json"].get("code", "")
        assert "SUBSELECT_NOT_ALLOWED" in resp["json"].get("code", "")


class TestCacheInvalidationTestFormulaMode(CacheInvalidationTestBase):
    """Tests for POST /api/data/v2/datasets/{ds_id}/cache_invalidation_test (Formula mode)"""

    raw_sql_level = RawSQLLevel.subselect

    def test_success_formula_mode(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        """Test successful cache invalidation test with formula mode (MAX on string field)."""
        update_data = {
            "mode": CacheInvalidationMode.formula.value,
            "field": {
                "guid": "cache_field_1",
                "formula": "MAX([category])",
            },
        }
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        resp = self._call_cache_invalidation_test(
            data_api=data_api,
            dataset_id=saved_dataset.id,
        )

        assert resp["status_code"] == HTTPStatus.OK, resp["json"]
        assert "result" in resp["json"]
        assert "value" in resp["json"]["result"]
        assert "query" in resp["json"]["result"]
        # MAX([category]) should return a non-empty string
        assert isinstance(resp["json"]["result"]["value"], str)
        assert len(resp["json"]["result"]["value"]) > 0

    def test_success_formula_mode_with_str_cast(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        """Test formula mode with STR() cast wrapping a numeric aggregation."""
        update_data = {
            "mode": CacheInvalidationMode.formula.value,
            "field": {
                "guid": "cache_field_1",
                "formula": "STR(SUM([sales]))",
            },
        }
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        resp = self._call_cache_invalidation_test(
            data_api=data_api,
            dataset_id=saved_dataset.id,
        )

        assert resp["status_code"] == HTTPStatus.OK, resp["json"]
        assert isinstance(resp["json"]["result"]["value"], str)
        # The value should be a numeric string
        assert len(resp["json"]["result"]["value"]) > 0

    def test_success_formula_mode_with_filter(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        """Test formula mode with a filter applied."""
        category_field = saved_dataset.find_field(title="category")

        update_data = {
            "mode": CacheInvalidationMode.formula.value,
            "field": {
                "guid": "cache_field_1",
                "formula": "MAX([category])",
            },
            "filters": [
                {
                    "id": "filter_1",
                    "field_guid": category_field.id,
                    "default_filters": [
                        {
                            "column": category_field.id,
                            "operation": WhereClauseOperation.EQ.name,
                            "values": ["Furniture"],
                        },
                    ],
                },
            ],
        }
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        resp = self._call_cache_invalidation_test(
            data_api=data_api,
            dataset_id=saved_dataset.id,
        )

        assert resp["status_code"] == HTTPStatus.OK, resp["json"]
        # With EQ filter on "Furniture", MAX([category]) should return "Furniture"
        assert resp["json"]["result"]["value"] == "Furniture"

    def test_success_formula_mode_with_multiple_filters(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        """Test formula mode with multiple filters applied."""
        category_field = saved_dataset.find_field(title="category")
        city_field = saved_dataset.find_field(title="city")

        update_data = {
            "mode": CacheInvalidationMode.formula.value,
            "field": {
                "guid": "cache_field_1",
                "formula": "MAX([category])",
            },
            "filters": [
                {
                    "id": "filter_1",
                    "field_guid": category_field.id,
                    "default_filters": [
                        {
                            "column": category_field.id,
                            "operation": WhereClauseOperation.EQ.name,
                            "values": ["Furniture"],
                        },
                    ],
                },
                {
                    "id": "filter_2",
                    "field_guid": city_field.id,
                    "default_filters": [
                        {
                            "column": city_field.id,
                            "operation": WhereClauseOperation.ICONTAINS.name,
                            "values": ["new"],
                        },
                    ],
                },
            ],
        }
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        resp = self._call_cache_invalidation_test(
            data_api=data_api,
            dataset_id=saved_dataset.id,
        )

        assert resp["status_code"] == HTTPStatus.OK, resp["json"]
        assert isinstance(resp["json"]["result"]["value"], str)
        assert len(resp["json"]["result"]["value"]) > 0

    def test_formula_mode_query_field_contains_sql(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        """Test that the 'query' field in the response contains the actual SQL sent to the database."""
        update_data = {
            "mode": CacheInvalidationMode.formula.value,
            "field": {
                "guid": "cache_field_1",
                "formula": "MAX([category])",
            },
        }
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        resp = self._call_cache_invalidation_test(
            data_api=data_api,
            dataset_id=saved_dataset.id,
        )

        assert resp["status_code"] == HTTPStatus.OK, resp["json"]
        query = resp["json"]["result"]["query"]
        # The query field should contain actual SQL, not the formula text
        assert "SELECT" in query.upper()
        assert "MAX" in query.upper()


class TestCacheInvalidationTestConnectionViewCheck(CacheInvalidationTestBase):
    """SQL test executed against a connection the caller can't view requires extra perm."""

    raw_sql_level = RawSQLLevel.subselect

    def _install_connection_read_denied(self, monkeypatch: pytest.MonkeyPatch) -> None:
        patched = _connection_read_denied_patch()
        monkeypatch.setattr(cache_invalidation_test_module, "check_permission_on_entry", patched)

    def _build_body_with_cache_invalidation_override(
        self,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
        cache_invalidation_source: dict,
    ) -> dict:
        """Build a full-dataset body (mirrors what the frontend sends) and override the
        cache_invalidation_source — sources/avatars/schema must come along so the
        endpoint's dataset patch produces a usable state."""
        body = data_api.serial_adapter.make_req_data_get_preview(dataset=saved_dataset)
        body["dataset"]["cache_invalidation_source"] = cache_invalidation_source
        return body

    def test_sql_unchanged_does_not_require_connection_view(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Saved SQL == request SQL: the connection-view check must not fire."""
        update_data = {"mode": CacheInvalidationMode.sql.value, "sql": "SELECT 'unchanged'"}
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        self._install_connection_read_denied(monkeypatch)

        # No body override → the endpoint uses the saved SQL verbatim.
        resp = self._call_cache_invalidation_test(
            data_api=data_api,
            dataset_id=saved_dataset.id,
        )

        assert resp["status_code"] == HTTPStatus.OK, resp["json"]
        assert resp["json"]["result"]["value"] == "unchanged"

    def test_sql_changed_via_body_requires_connection_view(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Body overrides SQL to a value different from US — connection-view perm required."""
        update_data = {"mode": CacheInvalidationMode.sql.value, "sql": "SELECT 'saved'"}
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        self._install_connection_read_denied(monkeypatch)

        body = self._build_body_with_cache_invalidation_override(
            data_api=data_api,
            saved_dataset=saved_dataset,
            cache_invalidation_source={
                "mode": CacheInvalidationMode.sql.value,
                "sql": "SELECT 'modified'",
            },
        )
        resp = self._call_cache_invalidation_test(
            data_api=data_api,
            dataset_id=saved_dataset.id,
            body=body,
        )

        assert resp["status_code"] == HTTPStatus.FORBIDDEN, resp["json"]
        assert "CACHE_INVALIDATION_TEST" in resp["json"].get("code", "")
        assert "CONNECTION_VIEW_REQUIRED" in resp["json"].get("code", "")

    def test_mode_flipped_to_sql_via_body_requires_connection_view(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Saved mode = formula, body flips to sql — connection-view perm required."""
        update_data = {
            "mode": CacheInvalidationMode.formula.value,
            "field": {"guid": "cache_field_1", "formula": "MAX([category])"},
        }
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        self._install_connection_read_denied(monkeypatch)

        body = self._build_body_with_cache_invalidation_override(
            data_api=data_api,
            saved_dataset=saved_dataset,
            cache_invalidation_source={
                "mode": CacheInvalidationMode.sql.value,
                "sql": "SELECT 'flipped'",
            },
        )
        resp = self._call_cache_invalidation_test(
            data_api=data_api,
            dataset_id=saved_dataset.id,
            body=body,
        )

        assert resp["status_code"] == HTTPStatus.FORBIDDEN, resp["json"]
        assert "CONNECTION_VIEW_REQUIRED" in resp["json"].get("code", "")

    def test_formula_mode_does_not_require_connection_view(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Saved + effective mode = formula: the SQL-mode connection-view check must not fire."""
        update_data = {
            "mode": CacheInvalidationMode.formula.value,
            "field": {"guid": "cache_field_1", "formula": "MAX([category])"},
        }
        saved_dataset = control_api.update_cache_invalidation(saved_dataset, update_data).dataset
        saved_dataset = control_api.save_dataset(saved_dataset).dataset

        self._install_connection_read_denied(monkeypatch)

        resp = self._call_cache_invalidation_test(
            data_api=data_api,
            dataset_id=saved_dataset.id,
        )

        assert resp["status_code"] == HTTPStatus.OK, resp["json"]
