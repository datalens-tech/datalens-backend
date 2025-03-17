import pytest

from dl_api_commons.base_models import RequestContextInfo
from dl_api_lib.dataset.view import DatasetView
from dl_api_lib.query.formalization.block_formalizer import BlockFormalizer
from dl_api_lib.query.formalization.legend_formalizer import ResultLegendFormalizer
from dl_api_lib.query.formalization.raw_specs import (
    IdFieldRef,
    RawQuerySpecUnion,
    RawSelectFieldSpec,
)
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_rls.testing.testing_data import (
    RLS_CONFIG_CASES,
    config_to_comparable,
    load_rls_config,
)


class TestRLS(DefaultApiTestBase):
    @staticmethod
    def add_rls_to_dataset(control_api, dataset, rls_config, rls2_config=None):
        field_guid = dataset.result_schema[0].id
        if rls2_config:
            dataset.rls2 = {field_guid: rls2_config}
            dataset.rls = {}
        else:
            dataset.rls = {field_guid: rls_config}
            dataset.rls2 = {}
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

    def test_create_rls_v2(self, control_api, saved_dataset):
        case = RLS_CONFIG_CASES[0]  # test only simple case
        config_v1 = case["config"]
        config_v2 = case["config_v2"]  # rls from v2 config != rls from v1 config

        ds = saved_dataset
        # if both config are passed v2 has priority
        field_guid, rls_resp = self.add_rls_to_dataset(control_api, ds, config_v1, config_v2)
        assert rls_resp.status_code == 200, rls_resp.json

        resp = control_api.load_dataset(ds)
        assert resp.status_code == 200, resp.json
        ds = resp.dataset

        # v2 has priority => check if v1 config is obtained from v2
        # 'config_to_compare_v2' - config_v2 data in v1 format, differs from 'config_to_compare'
        assert config_to_comparable(ds.rls[field_guid]) == config_to_comparable(case["config_to_compare_v2"])

        # check if v2 from rls looks like expected
        for item1, item2 in zip(sorted(ds.rls2[field_guid]), sorted(config_v2)):
            assert item1.subject == item2.subject
            assert item1.pattern_type == item2.pattern_type
            assert item1.allowed_value == item2.allowed_value

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

    def test_rls_filter_expr(self, control_api, saved_dataset, sync_us_manager):
        config = load_rls_config("dl_api_lib_test_config")
        field_a, field_b = saved_dataset.result_schema[0].id, saved_dataset.result_schema[1].id
        saved_dataset.rls = {field_a: config, field_b: config}
        saved_dataset.rls2 = {}
        control_api.save_dataset(saved_dataset, fail_ok=False)

        ds = sync_us_manager.get_by_id(saved_dataset.id)
        sync_us_manager.load_dependencies(ds)

        rci = RequestContextInfo(user_id="user1")
        raw_query_spec_union = RawQuerySpecUnion(
            select_specs=[
                RawSelectFieldSpec(ref=IdFieldRef(id=field_a)),
                RawSelectFieldSpec(ref=IdFieldRef(id=field_b)),
            ],
        )
        legend = ResultLegendFormalizer(dataset=ds).make_legend(raw_query_spec_union=raw_query_spec_union)
        block_legend = BlockFormalizer(dataset=ds).make_block_legend(
            raw_query_spec_union=raw_query_spec_union, legend=legend
        )
        ds_view = DatasetView(
            ds,
            us_manager=sync_us_manager,
            block_spec=block_legend.blocks[0],
            rci=rci,
        )

        exec_info = ds_view.build_exec_info()
        src_query = next(iter(exec_info.translated_multi_query.iter_queries()))
        assert len(src_query.where) == 2  # field_a in ... and field_b in ...
