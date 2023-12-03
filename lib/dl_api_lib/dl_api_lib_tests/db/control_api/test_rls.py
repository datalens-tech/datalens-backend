import pytest

from dl_api_lib_testing.rls import (
    RLS_CONFIG_CASES,
    config_to_comparable,
    load_rls_config,
)
from dl_api_lib_tests.db.base import DefaultApiTestBase


class TestRLS(DefaultApiTestBase):
    @staticmethod
    def add_rls_to_dataset(control_api, dataset, rls_config):
        field_guid = dataset.result_schema[0].id
        dataset.rls = {field_guid: rls_config}
        resp = control_api.save_dataset(dataset, fail_ok=True)
        return field_guid, resp

    def test_dataset_without_configured_rls(self, saved_dataset):
        assert all(rls_value == "" for rls_value in saved_dataset.rls.values())

    @pytest.mark.parametrize("case", RLS_CONFIG_CASES, ids=[c["name"] for c in RLS_CONFIG_CASES])
    def test_create_and_update_rls(self, control_api, saved_dataset, case):
        config = case["config"]
        ds = saved_dataset
        field_guid, rls_resp = self.add_rls_to_dataset(control_api, ds, config)
        assert rls_resp.status_code == 200, rls_resp.json

        resp = control_api.load_dataset(ds)
        assert resp.status_code == 200, resp.json
        ds = resp.dataset
        assert config_to_comparable(ds.rls[field_guid]) == config_to_comparable(case["config_to_compare"])

        config_updated = case.get("config_updated")
        if config_updated is None:
            return
        field_guid, rls_resp = self.add_rls_to_dataset(control_api, ds, config_updated)
        assert rls_resp.status_code == 200, rls_resp.json

        resp = control_api.load_dataset(ds)
        assert resp.status_code == 200, resp.json
        assert config_to_comparable(resp.dataset.rls[field_guid]) == config_to_comparable(config_updated)

    def test_create_rls_for_nonexistent_user(self, control_api, saved_dataset):
        config = load_rls_config("bad_login")
        ds = saved_dataset
        field_guid, rls_resp = self.add_rls_to_dataset(control_api, ds, config)
        assert rls_resp.status_code == 200, rls_resp.json

        resp = control_api.load_dataset(ds)
        assert resp.status_code == 200, resp.json
        assert "!FAILED_robot-user2" in resp.dataset.rls[field_guid]

    def test_create_rls_from_invalid_config(self, control_api, saved_dataset):
        config = load_rls_config("bad")
        _, rls_resp = self.add_rls_to_dataset(control_api, saved_dataset, config)

        assert rls_resp.status_code == 400
        assert rls_resp.bi_status_code == "ERR.DS_API.RLS.PARSE"
        assert rls_resp.json["message"] == "RLS: Parsing failed at line 2"
        assert rls_resp.json["details"] == {"description": "Wrong format"}
