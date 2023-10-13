from dl_api_client.dsmaker.primitives import ResultField
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import (
    AggregationFunction,
    CalcMode,
)


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

    def test_filter_errors(self, saved_dataset, data_api):
        ds = saved_dataset
        ds.result_schema["New date"] = ds.field(formula="DATE(NOW())")  # just a date field
        ds.result_schema["New bool"] = ds.field(formula='"P" = "NP"')  # just a bool field

        # invalid value for date
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title="New date")],
            filters=[ds.find_field(title="New date").filter("EQ", values=["Office Supplies"])],
            fail_ok=True,
        )
        assert result_resp.status_code == 400
        assert result_resp.bi_status_code == "ERR.DS_API.FILTER.INVALID_VALUE"

        # a very special check for invalid bool values
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title="New date")],
            filters=[ds.result_schema["New bool"].filter("EQ", values=[""])],
            fail_ok=True,
        )
        assert result_resp.status_code == 400
        assert result_resp.bi_status_code == "ERR.DS_API.FILTER.INVALID_VALUE"

        # unknown field
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title="New date")],
            filters=[ds.field(title="Made-up field").filter("EQ", values=["whatever"])],
            fail_ok=True,
        )
        assert result_resp.status_code == 400
        assert result_resp.bi_status_code == "ERR.DS_API.FIELD.NOT_FOUND"

    def test_invalid_group_by_configuration(self, saved_dataset, data_api):
        ds = saved_dataset
        ds.result_schema["Measure"] = ds.field(title="Measure", aggregation=AggregationFunction.sum)

        result_resp = data_api.get_result(
            dataset=saved_dataset,
            fields=[ds.find_field(title="Measure")],
            disable_group_by=True,
            fail_ok=True,
        )
        assert result_resp.status_code == 400
        assert result_resp.bi_status_code == "ERR.DS_API.INVALID_GROUP_BY_CONFIGURATION"
        assert result_resp.json["message"] == "Invalid parameter disable_group_by for dataset with measure fields"
