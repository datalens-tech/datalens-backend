import copy
import json

from dl_api_lib_tests.db.base import DefaultApiTestBase


class TestRevisionResult(DefaultApiTestBase):
    def test_query_revision_result(self, saved_dataset, data_api, control_api, sync_us_manager):
        ds = saved_dataset
        usm = sync_us_manager
        us_client = usm._us_client

        entry_revisions = us_client.get_entry_revisions(ds.id)
        assert len(entry_revisions) == 1
        rev_id = entry_revisions[-1]["revId"]

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="discount"),
                ds.find_field(title="city"),
            ],
            order_by=[
                ds.find_field(title="discount").desc,
            ],
            query_params={
                "rev_id": rev_id,
            },
        )
        assert result_resp.status_code == 200

    def test_prev_revision_result(self, saved_dataset, data_api, control_api, sync_us_manager):
        ds = saved_dataset
        usm = sync_us_manager
        us_client = usm._us_client

        entry_revisions = us_client.get_entry_revisions(ds.id)
        assert len(entry_revisions) == 1
        rev_id = entry_revisions[-1]["revId"]

        dataset = control_api.client.get("/api/v1/datasets/{}/versions/draft".format(ds.id)).json
        result_schema = copy.deepcopy(dataset["dataset"]["result_schema"])

        new_result_schema = [item for item in result_schema if item["title"] != "city"]

        resp = control_api.client.put(
            "/api/v1/datasets/{}/versions/draft".format(saved_dataset.id),
            data=json.dumps(
                {
                    "dataset": {
                        "result_schema": new_result_schema,
                    },
                }
            ),
            content_type="application/json",
        )
        assert resp.status_code == 200

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="discount"),
                ds.find_field(title="city"),
            ],
            order_by=[
                ds.find_field(title="discount").desc,
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == 400

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="discount"),
                ds.find_field(title="city"),
            ],
            order_by=[
                ds.find_field(title="discount").desc,
            ],
            query_params={
                "rev_id": rev_id,
            },
        )
        assert result_resp.status_code == 200
