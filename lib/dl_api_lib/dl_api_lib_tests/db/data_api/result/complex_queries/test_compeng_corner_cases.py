from __future__ import annotations

from http import HTTPStatus

import pytest

from dl_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_lib_testing.api_base import DefaultApiTestBase


class TestCompengCornerCases(DefaultApiTestBase):
    def test_zero_division_in_compeng(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Measure Reg Div": "1 / (SUM(SUM([sales]) AMONG) * 0)",
                "Measure Int Div": "DIV(1, SUM(COUNT([sales]) AMONG) * 0)",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="Measure Reg Div"),
                ds.find_field(title="Measure Int Div"),
            ],
            fail_ok=True,
        )

        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) == 1
        row = data_rows[0]
        assert row[0] is None
        assert row[1] is None

    def test_integer_division_in_compeng(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Measure": "COUNT([sales])",
                "Fraction": 'DB_CAST([Measure], "Int64") / DB_CAST(SUM([Measure] TOTAL), "integer")',
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="city"),
                ds.find_field(title="Fraction"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) > 1

        fraction_sum = sum(float(row[1]) for row in data_rows)
        assert pytest.approx(fraction_sum) == 1.0
