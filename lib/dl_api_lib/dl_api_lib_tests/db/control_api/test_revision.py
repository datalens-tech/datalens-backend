import copy
import json

from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_tests.db.base import DefaultApiTestBase


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
