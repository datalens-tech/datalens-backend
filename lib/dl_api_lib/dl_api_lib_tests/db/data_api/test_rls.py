import pytest

from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_lib_testing.app import TestingSubjectResolver
from dl_api_lib_testing.api_base import DefaultApiTestBase
from dl_rls.testing.testing_data import load_rls_config


class TestRLS(DefaultApiTestBase):
    @pytest.fixture(scope="function", params=[("dl_api_lib_test_config", False), ("dl_api_lib_group_config", True)])
    def dataset_with_rls(self, request, monkeypatch, control_api, saved_dataset):
        def get_groups_by_subject_mock(self, rci):
            raise RuntimeError("Shouldn't be invoked without groups in config")

        ds = saved_dataset
        field_guid = ds.result_schema[1].id
        config_file, contains_groups = request.param
        if not contains_groups:
            monkeypatch.setattr(TestingSubjectResolver, "get_groups_by_subject", get_groups_by_subject_mock)

        ds.rls = {field_guid: load_rls_config(config_file)}
        control_api.save_dataset(ds, fail_ok=False)
        resp = control_api.load_dataset(ds)
        assert resp.status_code == 200, resp.json
        return resp.dataset

    @staticmethod
    def _get_rls_preview_response(ds, data_api, monkeypatch, modify_rls):
        def get_subjects_by_names_mock(self, names):
            raise RuntimeError("Shouldn't be invoked in preview")

        rls_val_modifier = "\n'x': *\n" if modify_rls else ""
        ds.rls = {key: val + rls_val_modifier for key, val in ds.rls.items() if val}
        monkeypatch.setattr(TestingSubjectResolver, "get_subjects_by_names", get_subjects_by_names_mock)
        return data_api.get_preview(dataset=ds, fail_ok=True)

    def test_preview_with_saved_rls(self, dataset_with_rls, data_api, monkeypatch):
        resp = self._get_rls_preview_response(dataset_with_rls, data_api, monkeypatch, modify_rls=False)
        assert resp.status_code == 200, resp.json

        rls_data = [row[1] for row in get_data_rows(resp)]
        # ensure all values from the RLS config are presented and no other values are
        assert set(rls_data) == {"Naperville", "Philadelphia"}

    def test_preview_with_updated_rls(self, dataset_with_rls, data_api, monkeypatch):
        resp = self._get_rls_preview_response(dataset_with_rls, data_api, monkeypatch, modify_rls=True)
        resp_data = resp.json
        assert resp.status_code == 400, resp_data
        assert resp.bi_status_code == "ERR.DS_API.RLS.PARSE"
        assert resp_data["message"] == "For this feature to work, save dataset after editing the RLS config."

    def test_result_with_rls(self, dataset_with_rls, data_api):
        resp = data_api.get_result(dataset=dataset_with_rls, fields=[dataset_with_rls.result_schema[1]])
        assert resp.status_code == 200, resp.json
        data = [row[0] for row in get_data_rows(resp)]
        assert set(data) == {"Naperville", "Philadelphia"}
