import shortuuid

from dl_api_client.dsmaker.primitives import Dataset
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_core.base_models import PathEntryLocation


class TestDataset(DefaultApiTestBase):
    def test_invalid_dataset_id(self, control_api, saved_connection_id, saved_dataset, sync_us_manager):
        usm = sync_us_manager
        us_client = usm._us_client
        path = PathEntryLocation(shortuuid.uuid())
        dash = us_client.create_entry(scope="dash", key=path)
        dash_id = dash["entryId"]

        resp = control_api.client.get("/api/v1/datasets/{}/versions/draft".format(saved_dataset.id))
        assert resp.status_code == 200

        resp = control_api.client.get("/api/v1/datasets/{}/versions/draft".format(saved_connection_id))
        assert resp.status_code == 404

        resp = control_api.client.get("/api/v1/datasets/{}/versions/draft".format(dash_id))
        assert resp.status_code == 404

    def test_create_entity_with_existing_name(self, control_api, saved_connection_id, saved_dataset, dataset_params):
        name = saved_dataset.name

        second_ds = Dataset(name=name)
        second_ds.sources["source_1"] = second_ds.source(
            connection_id=saved_connection_id,
            **dataset_params,
        )
        second_ds.source_avatars["avatar_1"] = second_ds.sources["source_1"].avatar()

        second_ds = control_api.apply_updates(dataset=second_ds).dataset
        resp = control_api.save_dataset(dataset=second_ds, fail_ok=True)
        assert resp.status_code == 400
        assert resp.json["message"] == "The entry already exists"
        assert resp.json["code"] == "ERR.DS_API.US.BAD_REQUEST.ALREADY_EXISTS"
