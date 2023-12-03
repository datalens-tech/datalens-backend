import pytest

from dl_api_lib_testing.app import TestingSubjectResolver
from dl_api_lib_testing.rls import MAIN_TEST_CASE
from dl_api_lib_tests.db.base import DefaultApiTestBase


class TestRLS(DefaultApiTestBase):
    @pytest.fixture(scope="function")
    def dataset_with_rls(self, control_api, saved_dataset):
        ds = saved_dataset
        field_guid = ds.result_schema[0].id
        ds.rls = {field_guid: MAIN_TEST_CASE["config"]}

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
        return data_api.get_preview(dataset=ds, limit=13, fail_ok=True)

    def test_preview_with_saved_rls(self, dataset_with_rls, data_api, monkeypatch):
        resp = self._get_rls_preview_response(dataset_with_rls, data_api, monkeypatch, modify_rls=False)
        resp_data = resp.json
        assert resp.status_code == 200, resp_data
        assert resp_data

    def test_preview_with_updated_rls(self, dataset_with_rls, data_api, monkeypatch):
        resp = self._get_rls_preview_response(dataset_with_rls, data_api, monkeypatch, modify_rls=True)
        resp_data = resp.json
        assert resp.status_code == 400, resp_data
        assert resp.bi_status_code == "ERR.DS_API.RLS.PARSE"
        assert resp_data["message"] == "For this feature to work, save dataset after editing the RLS config."
