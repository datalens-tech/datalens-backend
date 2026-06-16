import copy
import json

from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants import USEntryMode


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


class TestControlApiRevisionHistory(DefaultApiTestBase):
    def test_entry_revisions(self, saved_dataset, sync_us_manager):
        usm = sync_us_manager
        us_client = usm._us_client

        resp = us_client.get_entry_revisions(saved_dataset.id)
        assert len(resp) == 1
        assert isinstance(resp[0]["revId"], str)

    def test_dataset_rev_id_parameter(self, control_api, saved_dataset, sync_us_manager):
        usm = sync_us_manager
        us_client = usm._us_client

        dataset = control_api.client.get(f"/api/v1/datasets/{saved_dataset.id}/versions/draft").json

        result_schema = copy.deepcopy(dataset["dataset"]["result_schema"])
        result_schema.append(dict(result_schema[1]))
        del result_schema[-1]["guid"]
        result_schema[-1]["title"] = "New Field"

        resp = control_api.client.put(
            f"/api/v1/datasets/{saved_dataset.id}/versions/draft",
            data=json.dumps(
                {
                    "dataset": {
                        "result_schema": result_schema,
                    },
                }
            ),
            content_type="application/json",
        )
        assert resp.status_code == 200

        upd_dataset = control_api.client.get(f"/api/v1/datasets/{saved_dataset.id}/versions/draft").json
        revision_id = upd_dataset["dataset"]["revision_id"]

        entry_revisions = us_client.get_entry_revisions(saved_dataset.id)
        assert len(entry_revisions) == 2

        old_rev_id = entry_revisions[-1]["revId"]

        resp = control_api.client.get(f"/api/v1/datasets/{saved_dataset.id}/versions/draft?rev_id={old_rev_id}")
        assert resp.status_code == 200

        old_dataset = resp.json
        assert old_dataset["dataset"]["result_schema"][-1]["title"] != "New Field"
        assert old_dataset["dataset"]["revision_id"] == revision_id

    def test_connection_rev_id_parameter(self, control_api, saved_connection_id, sync_us_manager):
        usm = sync_us_manager
        us_client = usm._us_client

        conn = control_api.client.get(f"/api/v1/connections/{saved_connection_id}").json
        username = conn["username"]

        control_api.client.put(
            f"/api/v1/connections/{saved_connection_id}",
            data=json.dumps(
                {
                    "username": "new_username",
                }
            ),
            content_type="application/json",
        )

        upd_conn = control_api.client.get(f"/api/v1/connections/{saved_connection_id}").json
        upd_username = upd_conn["username"]
        assert username != upd_username

        entry_revisions = us_client.get_entry_revisions(saved_connection_id)
        assert len(entry_revisions) == 2

        old_rev_id = entry_revisions[-1]["revId"]

        resp = control_api.client.get(f"/api/v1/connections/{saved_connection_id}?rev_id={old_rev_id}")
        assert resp.status_code == 200

        old_conn = resp.json
        assert old_conn["username"] == username

    def test_create_dataset_returns_us_revision_fields(self, control_api, saved_connection_id, dataset_params):
        ds = Dataset()
        ds.sources["s"] = ds.source(connection_id=saved_connection_id, **dataset_params)
        ds.source_avatars["a"] = ds.sources["s"].avatar()
        ds = control_api.apply_updates(dataset=ds).dataset

        resp = control_api.save_dataset(dataset=ds, fail_ok=True)
        assert resp.status_code == 200
        assert "revId" in resp.json
        assert "savedId" in resp.json
        assert "publishedId" in resp.json

        control_api.delete_dataset(dataset_id=resp.json["id"], fail_ok=False)

    def test_get_dataset_returns_us_revision_fields(self, control_api, saved_dataset):
        get_resp = control_api.client.get(f"/api/v1/datasets/{saved_dataset.id}/versions/draft")
        assert get_resp.status_code == 200
        assert "revId" in get_resp.json
        assert "savedId" in get_resp.json
        assert "publishedId" in get_resp.json

    def test_update_dataset_returns_us_revision_fields(self, control_api, saved_dataset):
        get_resp = control_api.client.get(f"/api/v1/datasets/{saved_dataset.id}/versions/draft")
        assert get_resp.status_code == 200
        result_schema = copy.deepcopy(get_resp.json["dataset"]["result_schema"])

        put_resp = control_api.client.put(
            f"/api/v1/datasets/{saved_dataset.id}/versions/draft",
            data=json.dumps({"dataset": {"result_schema": result_schema}}),
            content_type="application/json",
        )
        assert put_resp.status_code == 200
        assert "revId" in put_resp.json
        assert "savedId" in put_resp.json
        assert "publishedId" in put_resp.json

    def test_validate_dataset_returns_us_revision_fields(self, control_api, saved_dataset):
        resp = control_api.apply_updates(dataset=saved_dataset)
        assert resp.status_code == 200
        assert "revId" in resp.json
        assert "savedId" in resp.json
        assert "publishedId" in resp.json


class TestDraftRevisionConcurrentEditCheck(DefaultApiTestBase):
    """
    A draft (mode=save) PUT writes a new revision to the US `saved` branch only — the
    `published` branch keeps the older one. Loading the dataset by default returns the
    published branch, so the naive `dataset.revision_id != body.revision_id` check would
    always trip on the next request after a draft save. These tests pin the contract:
    validate and PUT after a draft save must succeed when the client sends back what the
    previous response gave it, and must still fail when the client carries a stale body.
    """

    def test_get_after_save_draft_returns_draft_revision_id(self, control_api, saved_dataset):
        """GET after a mode=save PUT must surface the saved-branch revision_id, not the published one."""
        before = _get_dataset(control_api, saved_dataset.id).json

        draft_resp = _put_dataset(control_api, saved_dataset.id, before["dataset"], mode=USEntryMode.save)
        assert draft_resp.status_code == 200, draft_resp.json
        saved_rev_id = draft_resp.json["dataset"]["revision_id"]
        assert saved_rev_id != before["dataset"]["revision_id"]

        after = _get_dataset(control_api, saved_dataset.id).json
        assert after["dataset"]["revision_id"] == saved_rev_id

    def test_validate_after_save_draft_does_not_raise_mismatch(self, control_api, saved_dataset):
        """The reported bug: validate after a draft save with the new revision_id must succeed."""
        before = _get_dataset(control_api, saved_dataset.id).json

        draft_resp = _put_dataset(control_api, saved_dataset.id, before["dataset"], mode=USEntryMode.save)
        assert draft_resp.status_code == 200, draft_resp.json

        validate_resp = _validate_dataset(control_api, saved_dataset.id, draft_resp.json["dataset"])
        assert validate_resp.status_code == 200, validate_resp.json

    def test_save_draft_after_save_draft_does_not_raise_mismatch(self, control_api, saved_dataset):
        """A second mode=save PUT must succeed when fed the previous response's body."""
        before = _get_dataset(control_api, saved_dataset.id).json

        first_resp = _put_dataset(control_api, saved_dataset.id, before["dataset"], mode=USEntryMode.save)
        assert first_resp.status_code == 200, first_resp.json
        first_saved_rev_id = first_resp.json["dataset"]["revision_id"]

        second_resp = _put_dataset(control_api, saved_dataset.id, first_resp.json["dataset"], mode=USEntryMode.save)
        assert second_resp.status_code == 200, second_resp.json
        assert second_resp.json["dataset"]["revision_id"] != first_saved_rev_id

    def test_publish_after_save_draft_does_not_raise_mismatch(self, control_api, saved_dataset):
        """Promoting a draft to published via mode=publish must succeed with the draft's body."""
        before = _get_dataset(control_api, saved_dataset.id).json

        draft_resp = _put_dataset(control_api, saved_dataset.id, before["dataset"], mode=USEntryMode.save)
        assert draft_resp.status_code == 200, draft_resp.json

        publish_resp = _put_dataset(control_api, saved_dataset.id, draft_resp.json["dataset"], mode=USEntryMode.publish)
        assert publish_resp.status_code == 200, publish_resp.json

    def test_validate_with_stale_body_after_save_draft_raises_mismatch(self, control_api, saved_dataset):
        """The guard must still fire when the client carries a pre-draft body."""
        stale = copy.deepcopy(_get_dataset(control_api, saved_dataset.id).json["dataset"])

        draft_resp = _put_dataset(control_api, saved_dataset.id, stale, mode=USEntryMode.save)
        assert draft_resp.status_code == 200, draft_resp.json
        assert draft_resp.json["dataset"]["revision_id"] != stale["revision_id"]

        validate_resp = _validate_dataset(control_api, saved_dataset.id, stale)
        assert validate_resp.status_code == 400
        assert "DATASET_REVISION_MISMATCH" in validate_resp.json["code"]

    def test_put_with_stale_body_after_save_draft_raises_mismatch(self, control_api, saved_dataset):
        """PUT must also still reject a stale body after a draft save."""
        stale = copy.deepcopy(_get_dataset(control_api, saved_dataset.id).json["dataset"])

        draft_resp = _put_dataset(control_api, saved_dataset.id, stale, mode=USEntryMode.save)
        assert draft_resp.status_code == 200, draft_resp.json

        put_resp = _put_dataset(control_api, saved_dataset.id, stale, mode=USEntryMode.publish)
        assert put_resp.status_code == 400
        assert "DATASET_REVISION_MISMATCH" in put_resp.json["code"]

    def test_validate_with_rev_id_query_param(self, control_api, saved_dataset, sync_us_manager):
        """The new ?rev_id=... query on validate must load the dataset at that historical revision."""
        us_client = sync_us_manager._us_client

        first_publish = _get_dataset(control_api, saved_dataset.id).json
        first_resp = _put_dataset(control_api, saved_dataset.id, first_publish["dataset"])
        assert first_resp.status_code == 200, first_resp.json

        # Second publish so we have an older revId to query.
        second_resp = _put_dataset(control_api, saved_dataset.id, first_resp.json["dataset"])
        assert second_resp.status_code == 200, second_resp.json

        entry_revisions = us_client.get_entry_revisions(saved_dataset.id)
        assert len(entry_revisions) >= 2
        old_us_rev_id = entry_revisions[-1]["revId"]

        # Simulate the real client flow: GET the historical revision, then post the resulting
        # body back to validate with the same rev_id query param.
        old_get_resp = control_api.client.get(
            f"/api/v1/datasets/{saved_dataset.id}/versions/draft?rev_id={old_us_rev_id}"
        )
        assert old_get_resp.status_code == 200

        validate_resp = _validate_dataset(
            control_api,
            saved_dataset.id,
            old_get_resp.json["dataset"],
            rev_id=old_us_rev_id,
        )
        assert validate_resp.status_code == 200, validate_resp.json

    def test_validate_without_query_param_does_not_raise(self, control_api, saved_dataset):
        """Regression guard for the KeyError when the new query schema produces an empty dict."""
        latest = _get_dataset(control_api, saved_dataset.id).json
        validate_resp = _validate_dataset(control_api, saved_dataset.id, latest["dataset"])
        assert validate_resp.status_code == 200, validate_resp.json
