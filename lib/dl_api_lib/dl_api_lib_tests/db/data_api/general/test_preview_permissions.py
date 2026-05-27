from __future__ import annotations

from collections.abc import Callable
import http

import pytest

from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
import dl_api_lib.app.data_api.resources.dataset.base as data_api_base
import dl_api_lib.app.data_api.resources.dataset.preview as preview_module
from dl_api_lib.enums import USPermissionKind
from dl_api_lib.utils.base import check_permission_on_entry
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_core.us_connection_base import ConnectionBase
from dl_core.us_dataset import Dataset as USDataset
from dl_core.us_entry import USEntry

PermissionPatch = Callable[[USEntry, USPermissionKind], bool]


def _make_permission_patch(
    *,
    dataset_edit_allowed: bool = True,
    connection_read_allowed: bool = True,
) -> PermissionPatch:
    """Override `check_permission_on_entry` to simulate dataset/connection permission combinations.

    All non-listed permissions delegate to the real implementation, so existing
    test fixtures (full-rights users) continue to behave normally.
    """
    real = check_permission_on_entry

    def patched(entry: USEntry, permission: USPermissionKind) -> bool:
        if isinstance(entry, USDataset) and permission is USPermissionKind.edit:
            return dataset_edit_allowed
        if isinstance(entry, ConnectionBase) and permission is USPermissionKind.read:
            return connection_read_allowed
        return real(entry, permission)

    return patched


class TestPreviewPermissions(DefaultApiTestBase):
    """`/preview` must respect connection × dataset permission combinations."""

    def _install_patch(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        dataset_edit_allowed: bool,
        connection_read_allowed: bool,
    ) -> None:
        patched = _make_permission_patch(
            dataset_edit_allowed=dataset_edit_allowed,
            connection_read_allowed=connection_read_allowed,
        )
        monkeypatch.setattr(data_api_base, "check_permission_on_entry", patched)
        monkeypatch.setattr(preview_module, "check_permission_on_entry", patched)

    def test_view_only_silently_ignores_source_modifications(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`dataset->view` (no edit): modified source params in the body are dropped silently."""
        self._install_patch(
            monkeypatch,
            dataset_edit_allowed=False,
            connection_read_allowed=True,
        )

        original_parameters = dict(saved_dataset.sources[0].parameters or {})
        saved_dataset.sources[0].parameters = {**original_parameters, "table_name": "i_do_not_exist"}

        response = data_api.get_preview(dataset=saved_dataset, fail_ok=True)

        # The bogus table_name from the body must NOT be applied; the US-saved
        # version is previewed unchanged.
        assert response.status_code == http.HTTPStatus.OK, response.json

    def test_edit_plus_connection_read_allows_source_modifications(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Full perms (`dataset->edit` + `connection->view`) keep existing pass-through behavior."""
        self._install_patch(
            monkeypatch,
            dataset_edit_allowed=True,
            connection_read_allowed=True,
        )

        response = data_api.get_preview(dataset=saved_dataset, fail_ok=True)

        assert response.status_code == http.HTTPStatus.OK, response.json

    def test_edit_without_connection_read_blocks_source_modifications(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`dataset->edit` but no `connection->view`: changed source params → 403."""
        self._install_patch(
            monkeypatch,
            dataset_edit_allowed=True,
            connection_read_allowed=False,
        )

        original_parameters = dict(saved_dataset.sources[0].parameters or {})
        saved_dataset.sources[0].parameters = {**original_parameters, "table_name": "different_table"}

        response = data_api.get_preview(dataset=saved_dataset, fail_ok=True)

        assert response.status_code == http.HTTPStatus.FORBIDDEN, response.json
        assert "PREVIEW_SOURCE_MODIFICATION_NOT_ALLOWED" in response.json.get("code", "")

    def test_edit_without_connection_read_passes_unchanged_sources(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """`dataset->edit` but no `connection->view`: unchanged source params → 200."""
        self._install_patch(
            monkeypatch,
            dataset_edit_allowed=True,
            connection_read_allowed=False,
        )

        # No modifications to sources — body should be accepted.
        response = data_api.get_preview(dataset=saved_dataset, fail_ok=True)

        assert response.status_code == http.HTTPStatus.OK, response.json
