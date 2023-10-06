from dl_api_client.dsmaker.primitives import ResultField
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import CalcMode


class TestResultErrors(DefaultApiTestBase):
    def test_empty_query(self, saved_dataset, data_api):
        result_resp = data_api.get_result(dataset=saved_dataset, fields=[], fail_ok=True)
        assert result_resp.status_code == 400
        assert result_resp.bi_status_code == "ERR.DS_API.EMPTY_QUERY"

    def test_get_nonexisting_field(self, saved_dataset, data_api):
        result_resp = data_api.get_result(dataset=saved_dataset, fields=[ResultField(title="unknown")], fail_ok=True)
        assert result_resp.status_code == 400
        assert result_resp.bi_status_code == "ERR.DS_API.FIELD.NOT_FOUND"

    def test_calcmode_formula_without_formula_field(self, saved_dataset, data_api):
        ds = saved_dataset
        title = "Not a formula"
        ds.result_schema[title] = ds.field(title=title, calc_mode=CalcMode.formula)

        result_resp = data_api.get_result(dataset=ds, fields=[ds.find_field(title)], fail_ok=True)
        assert result_resp.status_code == 400
        assert result_resp.bi_status_code == "ERR.DS_API.FORMULA.PARSE.UNEXPECTED_EOF.EMPTY_FORMULA"
