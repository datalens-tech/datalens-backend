import copy
import json

import pytest

from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_core.enums import USEntryMode


def _get_dataset(control_api, dataset_id: str):
    return control_api.client.get(f"/api/v1/datasets/{dataset_id}/versions/draft")


def _put_dataset(control_api, dataset_id: str, dataset_body: dict, mode: USEntryMode | None = None):
    body: dict = {"dataset": dataset_body}
    if mode is not None:
        body["mode"] = mode.value
    return control_api.client.put(
        f"/api/v1/datasets/{dataset_id}/versions/draft",
        data=json.dumps(body),
        content_type="application/json",
    )


def _validate_dataset(control_api, dataset_id: str, dataset_body: dict, rev_id: str | None = None):
    url = f"/api/v1/datasets/{dataset_id}/versions/draft/validators/schema"
    if rev_id is not None:
        url = f"{url}?rev_id={rev_id}"
    return control_api.client.post(
        url,
        data=json.dumps({"dataset": dataset_body}),
        content_type="application/json",
    )


class TestDraftRevisionConcurrentEditCheck(DefaultApiTestBase):
    def test_preview_draft_revision(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
    ):
        before = _get_dataset(control_api, saved_dataset.id).json

        draft_resp = _put_dataset(control_api, saved_dataset.id, before["dataset"], mode=USEntryMode.save)
        assert draft_resp.status_code == 200, draft_resp.json
        saved_rev_id = draft_resp.json["dataset"]["revision_id"]
        assert saved_rev_id != before["dataset"]["revision_id"]
        saved_us_rev_id = draft_resp.json["savedId"]

        draft_get_resp = control_api.client.get(
            f"/api/v1/datasets/{saved_dataset.id}/versions/draft?rev_id={saved_us_rev_id}"
        )
        assert draft_get_resp.status_code == 200
        draft_dataset_data = draft_get_resp.json["dataset"]

        preview_resp = data_api.get_response_for_dataset_preview(
            dataset_id=saved_dataset.id,
            raw_body={"dataset": draft_dataset_data},
        )
        assert preview_resp.status_code == 200

    @pytest.mark.parametrize(
        ("latest_revision_mode"),
        sorted(USEntryMode, key=lambda item: item.value),
        ids=sorted([mode.value for mode in USEntryMode]),
    )
    def test_validate_and_preview_old_revision(
        self,
        latest_revision_mode: USEntryMode,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
    ):
        before = _get_dataset(control_api, saved_dataset.id).json
        old_us_rev_id = before["revId"]

        latest_revision_resp = _put_dataset(control_api, saved_dataset.id, before["dataset"], mode=latest_revision_mode)
        assert latest_revision_resp.status_code == 200, latest_revision_resp.json
        latest_rev_id = latest_revision_resp.json["dataset"]["revision_id"]
        assert latest_rev_id != before["dataset"]["revision_id"]

        old_rev_get_resp = control_api.client.get(
            f"/api/v1/datasets/{saved_dataset.id}/versions/draft?rev_id={old_us_rev_id}"
        )
        assert old_rev_get_resp.status_code == 200
        old_rev_dataset_data = old_rev_get_resp.json["dataset"]

        validate_resp = _validate_dataset(control_api, saved_dataset.id, old_rev_dataset_data, old_us_rev_id)
        assert validate_resp.status_code == 200, validate_resp.json
        validated_dataset_data = validate_resp.json["dataset"]
        assert validated_dataset_data["revision_id"] == latest_rev_id

        preview_resp = data_api.get_response_for_dataset_preview(
            dataset_id=saved_dataset.id,
            raw_body={"dataset": validated_dataset_data},
        )
        assert preview_resp.status_code == 200

    def test_preview_draft_after_save_draft(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
    ):
        before = _get_dataset(control_api, saved_dataset.id).json

        draft_resp = _put_dataset(control_api, saved_dataset.id, before["dataset"], mode=USEntryMode.save)
        assert draft_resp.status_code == 200, draft_resp.json

        preview_resp = data_api.get_response_for_dataset_preview(
            dataset_id=saved_dataset.id,
            raw_body={"dataset": draft_resp.json["dataset"]},
        )
        assert preview_resp.status_code == 200

    def test_preview_published_after_save_draft(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
    ):
        before = _get_dataset(control_api, saved_dataset.id).json

        draft_resp = _put_dataset(control_api, saved_dataset.id, before["dataset"], mode=USEntryMode.save)
        assert draft_resp.status_code == 200, draft_resp.json

        published_ds_resp = _get_dataset(control_api, saved_dataset.id)
        preview_resp = data_api.get_response_for_dataset_preview(
            dataset_id=saved_dataset.id,
            raw_body={"dataset": published_ds_resp.json["dataset"]},
        )
        assert preview_resp.status_code == 200

    def test_preview_with_stale_body_after_save_draft_raises_mismatch(
        self,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        saved_dataset: Dataset,
    ):
        """The guard must still fire when the client carries a pre-draft body."""
        stale = copy.deepcopy(_get_dataset(control_api, saved_dataset.id).json["dataset"])

        draft_resp = _put_dataset(control_api, saved_dataset.id, stale, mode=USEntryMode.save)
        assert draft_resp.status_code == 200, draft_resp.json
        assert draft_resp.json["dataset"]["revision_id"] != stale["revision_id"]

        preview_resp = data_api.get_response_for_dataset_preview(
            dataset_id=saved_dataset.id,
            raw_body={"dataset": stale},
        )
        assert preview_resp.status_code == 400
        assert "DATASET_REVISION_MISMATCH" in preview_resp.json["code"]
