from collections.abc import Callable
import http

import pytest

from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
import dl_api_lib.app.control_api.resources.dataset_base as control_api_dataset_base
from dl_api_lib.enums import USPermissionKind
from dl_api_lib.utils.base import check_permission_on_entry
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_core.us_entry import USEntry


class TestValidatorViewOnly(DefaultApiTestBase):
    """BI-7362 — control-api validator must restrict mutations for view-only users."""

    @pytest.fixture
    def view_only(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Force `check_permission_on_entry(..., edit)` to return False. The
        # validator handler uses this exact predicate (via
        # `DatasetResource.is_dataset_edit_allowed`) to decide whether to
        # restrict mutations; patching the module-local binding in
        # `dataset_base` is the smallest hook that targets the call site
        # without standing up real per-user US tokens.
        real_check: Callable[[USEntry, USPermissionKind], bool] = check_permission_on_entry

        def patched(entry: USEntry, permission: USPermissionKind) -> bool:
            if permission is USPermissionKind.edit:
                return False
            return real_check(entry, permission)

        monkeypatch.setattr(control_api_dataset_base, "check_permission_on_entry", patched)

    def test_view_only_allows_add_field(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        view_only: None,
    ) -> None:
        a_field = saved_dataset.result_schema[0]

        response = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[
                {
                    "action": "add_field",
                    "field": {
                        "guid": "view_only_added_field",
                        "title": "Copy {}".format(a_field.title),
                        "calc_mode": "formula",
                        "formula": "[{}]".format(a_field.title),
                    },
                }
            ],
            fail_ok=True,
        )

        assert response.status_code == http.HTTPStatus.OK, response.response_errors

    def test_view_only_blocks_delete_source(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        view_only: None,
    ) -> None:
        source_id = saved_dataset.sources[0].id

        response = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[
                {
                    "action": "delete_source",
                    "source": {"id": source_id},
                }
            ],
            fail_ok=True,
        )

        assert response.status_code == http.HTTPStatus.BAD_REQUEST
        assert response.bi_status_code == "ERR.DS_API.ACTION_NOT_ALLOWED"

    def test_view_only_blocks_rls_change_in_body(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
        view_only: None,
    ) -> None:
        # Force a non-trivial RLS diff. The loader compares saved (empty) vs
        # incoming and raises RLSConfigParsingError when allow_rls_change=False,
        # which is what we expect for view-only callers.
        field_guid = saved_dataset.result_schema[0].id
        saved_dataset.rls = {field_guid: "'Moscow': user1"}

        response = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[],
            fail_ok=True,
        )

        assert response.status_code == http.HTTPStatus.BAD_REQUEST
        assert response.bi_status_code == "ERR.DS_API.RLS.PARSE"

    def test_edit_user_can_change_rls(
        self,
        control_api: SyncHttpDatasetApiV1,
        saved_dataset: Dataset,
    ) -> None:
        field_guid = saved_dataset.result_schema[0].id
        saved_dataset.rls = {field_guid: "'Moscow': user1"}

        response = control_api.apply_updates(
            dataset=saved_dataset,
            updates=[],
            fail_ok=True,
        )

        assert response.status_code == http.HTTPStatus.OK, response.response_errors
