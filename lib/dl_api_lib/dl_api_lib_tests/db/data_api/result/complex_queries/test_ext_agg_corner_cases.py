from __future__ import annotations

from http import HTTPStatus

import pytest

from dl_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_lib_tests.db.base import DefaultApiTestBase


class TestExtendedAggregationCornerCases(DefaultApiTestBase):
    def test_lod_with_const_dim(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Const Dim": '"something"',
                "Measure": "SUM([sales] EXCLUDE [category])",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="category"),
                ds.find_field(title="Const Dim"),
                ds.find_field(title="Measure"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)

        assert len(data_rows) == 3
        assert len(set(row[2] for row in data_rows)) == 1  # They should all be the same

    def test_lod_with_date_dimension(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Date Dim": 'DB_CAST([order_date], "Date") + 1',
                "Measure": "SUM([sales] EXCLUDE [category])",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="category"),
                ds.find_field(title="Date Dim"),
                ds.find_field(title="Measure"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)

        assert len(data_rows) > 1

    def test_duplicate_main_dimensions(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Category 2": "[category]",
                "Measure": "AVG(SUM([sales] INCLUDE [city]))",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="category"),
                ds.find_field(title="Category 2"),
                ds.find_field(title="Measure"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) == 3  # There are 3 categories
        for row in data_rows:
            assert row[0] == row[1]

    def test_duplicate_lod_dimensions(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "City 2": "[city]",
                "Measure Double Dim": "AVG(SUM([sales] INCLUDE [city], [City 2]))",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="category"),
                ds.find_field(title="Measure Double Dim"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) == 3  # There are 3 categories

    def test_lod_include_measure(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Measure": "SUM(SUM([sales] INCLUDE SUM([profit])))",
            },
            exp_status=HTTPStatus.BAD_REQUEST,
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="category"),
                ds.find_field(title="Measure"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.BAD_REQUEST

    @pytest.mark.parametrize("function", ["COUNT()", "NOW()"])
    def test_lod_with_avatarless_function(self, control_api, data_api, saved_dataset, function):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Agg": "SUM(SUM([sales] FIXED [region]))",
                "Agg with avatarless function": f"CONCAT({function}, ': ', [Agg])",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="Agg with avatarless function"),
            ],
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        rows = get_data_rows(result_resp)
        assert len(rows) == 1
