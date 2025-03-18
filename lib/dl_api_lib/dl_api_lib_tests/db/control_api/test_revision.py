import copy
import json

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

        dataset = control_api.client.get("/api/v1/datasets/{}/versions/draft".format(saved_dataset.id)).json

        result_schema = copy.deepcopy(dataset["dataset"]["result_schema"])
        result_schema.append(dict(result_schema[1]))
        del result_schema[-1]["guid"]
        result_schema[-1]["title"] = "New Field"

        resp = control_api.client.put(
            "/api/v1/datasets/{}/versions/draft".format(saved_dataset.id),
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

        upd_dataset = control_api.client.get("/api/v1/datasets/{}/versions/draft".format(saved_dataset.id)).json
        revision_id = upd_dataset["dataset"]["revision_id"]

        entry_revisions = us_client.get_entry_revisions(saved_dataset.id)
        assert len(entry_revisions) == 2

        old_rev_id = entry_revisions[-1]["revId"]

        resp = control_api.client.get(
            "/api/v1/datasets/{}/versions/draft?rev_id={}".format(saved_dataset.id, old_rev_id)
        )
        assert resp.status_code == 200

        old_dataset = resp.json
        assert old_dataset["dataset"]["result_schema"][-1]["title"] != "New Field"
        assert old_dataset["dataset"]["revision_id"] == revision_id

    def test_connection_rev_id_parameter(self, control_api, saved_connection_id, sync_us_manager):
        usm = sync_us_manager
        us_client = usm._us_client

        conn = control_api.client.get("/api/v1/connections/{}".format(saved_connection_id)).json
        username = conn["username"]

        control_api.client.put(
            "/api/v1/connections/{}".format(saved_connection_id),
            data=json.dumps(
                {
                    "username": "new_username",
                }
            ),
            content_type="application/json",
        )

        upd_conn = control_api.client.get("/api/v1/connections/{}".format(saved_connection_id)).json
        upd_username = upd_conn["username"]
        assert username != upd_username

        entry_revisions = us_client.get_entry_revisions(saved_connection_id)
        assert len(entry_revisions) == 2

        old_rev_id = entry_revisions[-1]["revId"]

        resp = control_api.client.get("/api/v1/connections/{}?rev_id={}".format(saved_connection_id, old_rev_id))
        assert resp.status_code == 200

        old_conn = resp.json
        assert old_conn["username"] == username
