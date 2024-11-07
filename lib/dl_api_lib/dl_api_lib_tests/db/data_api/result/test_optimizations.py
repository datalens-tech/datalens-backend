from dl_api_client.dsmaker.primitives import StringParameterValue
from dl_api_client.dsmaker.shortcuts.dataset import (
    add_formulas_to_dataset,
    add_parameters_to_dataset,
)
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import WhereClauseOperation


class TestOptimizations(DefaultApiTestBase):
    const_value = "qweqwerty"

    def test_ignore_constant_filter(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={"Const": f'"{self.const_value}"'},
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="city").as_req_legend_item(),
            ],
            filters=[
                ds.find_field(title="Const").filter(WhereClauseOperation.EQ, [self.const_value]),
                ds.find_field(title="category").filter(WhereClauseOperation.EQ, ["Furniture"]),
            ],
            limit=1,
            fail_ok=True,
        )
        assert result_resp.status_code == 200, result_resp.json
        query = result_resp.json["blocks"][0]["query"]
        assert "Furniture" in query
        assert self.const_value not in query
        assert "and" not in query.lower()

    def test_ignore_parameter_filter(self, control_api, data_api, saved_dataset):
        ds = add_parameters_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            parameters={
                "Param": (StringParameterValue(self.const_value), None),
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="city").as_req_legend_item(),
            ],
            filters=[
                ds.find_field(title="Param").filter(WhereClauseOperation.EQ, [self.const_value]),
                ds.find_field(title="category").filter(WhereClauseOperation.EQ, ["Furniture"]),
            ],
            limit=1,
            fail_ok=True,
        )
        assert result_resp.status_code == 200, result_resp.json
        query = result_resp.json["blocks"][0]["query"]
        assert "Furniture" in query
        assert self.const_value not in query
        assert "and" not in query.lower()

    def test_optimize_zero_one_comparison(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "eq": "[category] = 'Furniture'",
                "geq": "[sales] >= 300",
                "in": "[category] in ('Furniture', 'Technology')",
                "not in": "[city] not in ('Moscow', 'London')",
                "or": "[category] = 'Furniture' OR [sales] >= 300 OR [sales] < 200",
                "and": "[sales] < 300 AND [sales] >= 200 AND [category] = 'Furniture'",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="city").as_req_legend_item(),
            ],
            filters=[
                ds.find_field(title="eq").filter(WhereClauseOperation.EQ, [True]),
                ds.find_field(title="geq").filter(WhereClauseOperation.EQ, [False]),
                ds.find_field(title="in").filter(WhereClauseOperation.EQ, [False]),
                ds.find_field(title="not in").filter(WhereClauseOperation.EQ, [True]),
                ds.find_field(title="or").filter(WhereClauseOperation.EQ, [True]),
                ds.find_field(title="and").filter(WhereClauseOperation.EQ, [False]),
            ],
            limit=1,
            fail_ok=True,
        )
        assert result_resp.status_code == 200, result_resp.json
        query = result_resp.json["blocks"][0]["query"]

        # ensure no form of true/false comparison is presented; this also covers !=
        for comparison in ("= 0", "= 1", "= true", "= false"):
            assert comparison not in query.lower()

        # ensure correct filters are presented
        assert "!= 'Furniture'" not in query
        assert "= 'Furniture'" in query
        assert "< 300" in query
        assert "NOT IN ('Furniture', 'Technology')" in query
        assert "NOT IN ('Moscow', 'London')" in query
        assert query.count("NOT (") == 1  # from [and], not from [or]
