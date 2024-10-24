from __future__ import annotations

from collections import defaultdict
from http import HTTPStatus

import pytest

from dl_api_client.dsmaker.api.data_api import HttpDataApiResponse
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_lib_testing.connector.complex_queries import DefaultBasicWindowFunctionTestSuite
from dl_api_lib_testing.helpers.data_source import data_source_settings_from_table
from dl_api_lib_testing.helpers.multi_query import (
    MultiQueryInterceptor,
    count_joins,
)
from dl_api_lib_testing.api_base import DefaultApiTestBase
from dl_constants.enums import (
    OrderDirection,
    UserDataType,
    WhereClauseOperation,
)
from dl_core_testing.database import (
    C,
    make_table,
)
from dl_query_processing.compilation.primitives import CompiledMultiQueryBase
from dl_query_processing.enums import ExecutionLevel


class TestBasicWindowFunctions(DefaultApiTestBase, DefaultBasicWindowFunctionTestSuite):
    def test_winfunc_lod_combination(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Sales Sum": "SUM([sales])",
                "Win Func": "SUM(SUM([sales]) TOTAL)",
                "Lod Func": "SUM(SUM([sales]) FIXED)",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="region"),
                ds.find_field(title="Sales Sum"),
                ds.find_field(title="Win Func"),
                ds.find_field(title="Lod Func"),
            ],
            fail_ok=True,
        )

        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)

        total_value = sum(float(row[1]) for row in data_rows)

        for row in data_rows:
            win_value = float(row[2])
            lod_value = float(row[3])
            assert pytest.approx(win_value) == total_value
            assert pytest.approx(lod_value) == total_value

    def test_window_function_filter(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Group Sales": "SUM([sales])",
                "Rank of Sales": 'RANK_UNIQUE([Group Sales], "asc" TOTAL)',
            },
        )

        def get_result(filters: list, window_in_select: bool = True) -> HttpDataApiResponse:
            select_fields = [
                ds.find_field(title="Group Sales"),
                ds.find_field(title="Rank of Sales"),
            ]
            if not window_in_select:
                # for testing queries with window functions only in the filter section
                select_fields = select_fields[:-1]

            result_resp = data_api.get_result(
                dataset=ds,
                fields=select_fields
                + [
                    ds.find_field(title="order_date"),
                    ds.find_field(title="city"),
                ],
                filters=filters,
                fail_ok=True,
            )
            return result_resp

        result_resp = get_result(
            filters=[
                ds.find_field(title="Rank of Sales").filter(WhereClauseOperation.LTE, [5]),
            ]
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) == 5

        # Combine with filters before window functions
        result_resp = get_result(
            filters=[
                # This one goes after
                ds.find_field(title="Rank of Sales").filter(WhereClauseOperation.LTE, [5]),
                # This one goes before
                ds.find_field(title="city").filter(WhereClauseOperation.STARTSWITH, ["S"]),
            ]
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) == 5

        # Without window functions in SELECT
        result_resp = get_result(
            filters=[
                ds.find_field(title="Rank of Sales").filter(WhereClauseOperation.LTE, [5]),
            ],
            window_in_select=False,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) == 5

    def test_window_function_filter_with_before_filter_by(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Group Sales": "SUM([sales])",
                "Rank of Sales BFB": 'RANK_UNIQUE([Group Sales], "asc" TOTAL BEFORE FILTER BY [city])',
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="city"),
                ds.find_field(title="Group Sales"),
                ds.find_field(title="Rank of Sales BFB"),
            ],
            filters=[
                ds.find_field(title="city").filter(WhereClauseOperation.STARTSWITH, ["S"]),
            ],
            fail_ok=True,
        )

        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        max_rank = 0
        possible_ranks = list(range(10000))
        for row in data_rows:
            assert row[1].startswith("S")
            row_rank = int(row[3])
            max_rank = max(max_rank, row_rank)
            possible_ranks.remove(row_rank)
        # rank should have gaps because of filters, so the max rank will be much greater than the number of rows
        assert max_rank > len(data_rows)
        possible_ranks = [r for r in possible_ranks if r <= max_rank]
        # ranks that were skipped should have remained in the list
        assert possible_ranks  # <- gaps

    def test_window_function_filter_with_before_filter_by_in_ordered_window_function(
        self, control_api, data_api, saved_dataset
    ):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Group Sales": "SUM([sales])",
                "RSUM of Sales BFB": "RSUM([Group Sales] BEFORE FILTER BY [order_date])",
                "MAVG of RSUM of Sales": "MAVG([RSUM of Sales BFB], 50)",
                "MAVG of MAVG of RSUM of Sales": "MAVG([MAVG of RSUM of Sales], 50)",
                "RSUM of Sales non-BFB": "RSUM([Group Sales])",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="Group Sales"),
                ds.find_field(title="RSUM of Sales BFB"),
                ds.find_field(title="order_date"),
                ds.find_field(title="MAVG of MAVG of RSUM of Sales"),
                ds.find_field(title="RSUM of Sales non-BFB"),
            ],
            order_by=[
                ds.find_field(title="order_date"),
            ],
            filters=[
                ds.find_field(title="order_date").filter(WhereClauseOperation.GTE, ["2015-05-01"]),
            ],
            fail_ok=True,
        )

        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        first_row = data_rows[0]
        # `* 2` - to eliminate float errors
        assert float(first_row[1]) > float(first_row[0]) * 2  # [RSUM of Sales BFB] > [Group Sales] * 2
        # Make sure the non-BFB field was calculated correctly too
        assert pytest.approx(float(first_row[4])) == float(first_row[0])  # [RSUM of Sales non-BFB] == [Group Sales]

    def test_order_dependent_window_function(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Group Sales": "SUM([sales])",
                "RSUM ASC": 'RSUM([Group Sales], "asc" TOTAL)',
                "RSUM DESC": 'RSUM([Group Sales], "desc" TOTAL)',
            },
        )

        def _get_data(direction: OrderDirection) -> list[list]:
            order_field = ds.find_field(title="city")
            result_resp = data_api.get_result(
                dataset=ds,
                fields=[
                    ds.find_field(title="city"),
                    ds.find_field(title="Group Sales"),
                    ds.find_field(title="RSUM ASC"),
                    ds.find_field(title="RSUM DESC"),
                ],
                order_by=[
                    order_field if direction == OrderDirection.asc else order_field.desc,
                ],
                fail_ok=True,
            )
            assert result_resp.status_code == HTTPStatus.OK, result_resp.json
            data_rows = get_data_rows(result_resp)
            return data_rows

        # ORDER BY City ASC
        data_1 = _get_data(OrderDirection.asc)
        city_values_1 = [row[0] for row in data_1]
        sum_values_1 = [float(row[1]) for row in data_1]
        rsum_asc_values_1 = [float(row[2]) for row in data_1]
        rsum_desc_values_1 = [float(row[3]) for row in data_1]
        assert city_values_1[0] == min(city_values_1)
        assert city_values_1[-1] == max(city_values_1)
        assert all(
            pytest.approx(rsum_asc_values_1[i]) == rsum_asc_values_1[i - 1] + sum_values_1[i]
            for i in range(1, len(data_1))
        )
        assert all(
            pytest.approx(rsum_desc_values_1[i]) == rsum_desc_values_1[i - 1] - sum_values_1[i - 1]
            for i in range(1, len(data_1))
        )

        # ORDER BY City DESC
        # (all of the patterns are the same except for the actual values of RSUM and the order of cities)
        data_2 = _get_data(OrderDirection.desc)
        city_values_2 = [row[0] for row in data_2]
        sum_values_2 = [float(row[1]) for row in data_2]
        rsum_asc_values_2 = [float(row[2]) for row in data_2]
        rsum_desc_values_2 = [float(row[3]) for row in data_2]
        assert city_values_2[0] == max(city_values_2)
        assert city_values_2[-1] == min(city_values_2)
        assert all(
            pytest.approx(rsum_asc_values_2[i]) == rsum_asc_values_2[i - 1] + sum_values_2[i]
            for i in range(1, len(data_2))
        )
        assert all(
            pytest.approx(rsum_desc_values_2[i]) == rsum_desc_values_2[i - 1] - sum_values_2[i - 1]
            for i in range(1, len(data_2))
        )

        assert pytest.approx(rsum_asc_values_2) == rsum_desc_values_1[::-1]
        assert pytest.approx(rsum_desc_values_2) == rsum_asc_values_1[::-1]

    def test_order_by_window_function(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Group Sales": "SUM([sales])",
                "Sales Rank": 'RANK_UNIQUE([Group Sales], "asc")',
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="city"),
                ds.find_field(title="Group Sales"),
                ds.find_field(title="Sales Rank"),
            ],
            order_by=[
                ds.find_field(title="Sales Rank"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)

        values = [int(row[2]) for row in data_rows]
        assert values == list(range(1, len(values) + 1))

    def test_nested_window_functions(self, control_api, data_api, saved_dataset):
        scary_formula_tmpl = 'RANK_UNIQUE(10 * {repl}, "asc")'
        scary_formula = scary_formula_tmpl
        for _ in range(10):
            scary_formula = scary_formula_tmpl.replace("{repl}", scary_formula)
        scary_formula = scary_formula.replace("{repl}", "[Group Sales]")

        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Group Sales": "SUM([sales])",
                "Rank of Sales": 'RANK_UNIQUE([Group Sales], "asc" TOTAL)',
                "Rank of Rank of Sales": 'RANK_UNIQUE([Rank of Sales], "desc" TOTAL)',
                "RSUM of Rank of Sales": "RSUM([Rank of Sales] TOTAL ORDER BY [Rank of Sales])",
                "10x Rank of Sales": scary_formula,
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="Group Sales"),
                ds.find_field(title="Rank of Sales"),
                ds.find_field(title="Rank of Rank of Sales"),
                ds.find_field(title="RSUM of Rank of Sales"),
                ds.find_field(title="10x Rank of Sales"),
            ],
            order_by=[
                ds.find_field(title="Rank of Sales"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)

        total_cnt = len(data_rows)
        rsum = 0
        for i, row in enumerate(data_rows):
            num = i + 1
            rsum += num
            assert int(row[2]) == num
            assert int(row[3]) == total_cnt - i
            assert int(row[4]) == rsum
            assert int(row[5]) == num

    def test_window_function_bfb_unselected_dimension_error(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Rank of Sales BFB": 'RANK_UNIQUE(SUM([sales]), "asc" TOTAL BEFORE FILTER BY [city])',
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="Rank of Sales BFB"),
            ],
            filters=[
                ds.find_field(title="city").filter(WhereClauseOperation.STARTSWITH, ["S"]),
            ],
            fail_ok=True,
        )

        assert result_resp.status_code == HTTPStatus.BAD_REQUEST, result_resp.json
        assert result_resp.json["code"] == "ERR.DS_API.FORMULA.VALIDATION.WIN_FUNC.BFB_UNSELECTED_DIMENSION"
        assert "neither an aggregation nor a dimension in the query" in result_resp.json["message"]

    def test_order_by_field_that_depends_on_window_function(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Group Sales": "SUM([sales])",
                "Rank": "RANK_UNIQUE([Group Sales])",
                "Stringified Rank": 'CONCAT("Rank ", [Rank])',
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="city"),
                ds.find_field(title="Rank"),
            ],
            order_by=[
                ds.find_field(title="Stringified Rank"),
            ],
            fail_ok=True,
        )

        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert data_rows

    def test_dimensions_in_window_function_identical_to_dims_in_query(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "City Clone": "[city]",
                "order_date Clone": "[order_date]",
                "Group Sales": "SUM([sales])",
                "RSum Among Clone": "RSUM([Group Sales] AMONG [City Clone])",
                "RSum Within Clone": "RSUM([Group Sales] WITHIN [order_date Clone])",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="city"),
                ds.find_field(title="order_date"),
                ds.find_field(title="RSum Among Clone"),
                ds.find_field(title="RSum Within Clone"),
            ],
            order_by=[
                ds.find_field(title="city"),
                ds.find_field(title="order_date"),
            ],
            fail_ok=True,
        )

        assert result_resp.status_code == HTTPStatus.OK, result_resp.json

    def test_order_by_multilevel_window_function(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Group Sales": "SUM([sales])",
                "Multi Rank": 'RANK(RANK(RANK(RANK_UNIQUE([Group Sales], "asc"), "asc"), "asc"), "asc")',
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="city"),
                ds.find_field(title="Group Sales"),
            ],
            order_by=[
                ds.find_field(title="Multi Rank"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json

    def test_extra_dimensions_in_within(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Value": "SUM(SUM([sales]) WITHIN [category], [city])",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="category"),
                ds.find_field(title="Value"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert data_rows
        by_cat: dict[str, set[str]] = defaultdict(set)
        for row in data_rows:
            by_cat[row[0]].add(row[1])

        assert len(by_cat) == 3
        # Values should be equivalent to `SUM(SUM([sales]) WITHIN [Category])`,
        # so all values within a category should be the same
        for cat_values in by_cat.values():
            assert len(cat_values) == 1

    def test_measure_in_within(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "My Measure": "COUNT()",
                "Win Value": "SUM(SUM([sales]) WITHIN [My Measure])",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="city"),
                ds.find_field(title="My Measure"),
                ds.find_field(title="Win Value"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert data_rows

        # Make sure that all `Win Value` values are the same for matching `My Measure` values
        by_my_measure = {row[2]: row[1] for row in data_rows}
        for row in data_rows:
            assert row[1] == by_my_measure[row[2]]

    def test_dim_complex_dimension_in_within(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Measure": "SUM([sales])",
                "Complex Dim": 'DATETRUNC([order_date], "week")',
                "Win Value": "SUM([Measure] WITHIN [Complex Dim])",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="Win Value"),
            ],
            order_by=[
                ds.find_field(title="order_date"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        date_values = {row[0] for row in data_rows}
        measure_values = {row[1] for row in data_rows}
        assert len(date_values) > len(measure_values) > 10  # the last is an arbitrary nonzero number

    def test_nested_among(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Nested Among": "SUM(RANK(SUM([sales]) AMONG [category]) AMONG [category])",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="region"),
                ds.find_field(title="category"),
                ds.find_field(title="Nested Among"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert data_rows

        for row in data_rows:
            # 3 Categories in each Region => RANK = 1,2,3; SUM(RANK) = 6
            assert int(row[2]) == 6

    def test_null_dimensions(self, control_api, data_api, saved_connection_id, db):
        raw_data = [
            {"id": 1, "city": "New York", "category": "Office Supplies", "sales": 1},
            {"id": 2, "city": "New York", "category": "Furniture", "sales": 10},
            {"id": 3, "city": "New Rochelle", "category": "Office Supplies", "sales": 100},
            {"id": 4, "city": "New Rochelle", "category": "Furniture", "sales": 1000},
            {"id": 5, "city": None, "category": "Office Supplies", "sales": 10000},
            {"id": 6, "city": None, "category": "Furniture", "sales": 100000},
        ]
        columns = [
            C("id", UserDataType.integer, vg=lambda rn, **kwargs: raw_data[rn]["id"]),
            C("city", UserDataType.string, vg=lambda rn, **kwargs: raw_data[rn]["city"]),
            C("category", UserDataType.string, vg=lambda rn, **kwargs: raw_data[rn]["category"]),
            C("sales", UserDataType.integer, vg=lambda rn, **kwargs: raw_data[rn]["sales"]),
        ]
        db_table = make_table(db, columns=columns, rows=len(raw_data))

        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            connection_id=saved_connection_id, **data_source_settings_from_table(db_table)
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()

        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=ds,
            formulas={
                "rank_within_cat": "RANK(SUM([sales]) WITHIN [category])",
                "rank_within_city": "RANK(SUM([sales]) WITHIN [city])",
                "max_id": "MAX([id])",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="city"),
                ds.find_field(title="category"),
                ds.find_field(title="rank_within_cat"),
                ds.find_field(title="rank_within_city"),
            ],
            order_by=[
                ds.find_field(title="max_id"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert data_rows

        city = [row[0] for row in data_rows]
        rank_within_cat = [int(row[2]) for row in data_rows]
        rank_within_city = [int(row[3]) for row in data_rows]

        assert city == ["New York", "New York", "New Rochelle", "New Rochelle", None, None]
        assert rank_within_cat == [3, 3, 2, 2, 1, 1]
        assert rank_within_city == [2, 1, 2, 1, 2, 1]

    def test_compeng_part_has_no_joins(self, control_api, data_api, saved_dataset, monkeypatch):
        """
        Check that when selecting:
        1. dimension,
        2. aggregation and
        3. window function
        in one simple request, there are no JOINs in compeng
        """

        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Sales Sum": "SUM([sales])",
                "Rank of Sales Sum": 'RANK_UNIQUE(SUM([sales]), "asc" TOTAL)',
            },
        )

        def intercept_query(multi_query: CompiledMultiQueryBase) -> None:
            compeng_part = multi_query.for_level_type(ExecutionLevel.compeng)
            assert compeng_part.query_count() > 1
            assert count_joins(compeng_part) == 0

        interceptor = MultiQueryInterceptor(mpatch=monkeypatch, callback=intercept_query)

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="region"),
                ds.find_field(title="Sales Sum"),
                ds.find_field(title="Rank of Sales Sum"),
            ],
            fail_ok=True,
        )

        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        assert interceptor.intercepted

    def test_wf_and_non_wf_in_filter(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Rank": "RANK(SUM([sales]) TOTAL)",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
            ],
            filters=[
                ds.find_field(title="city").filter(WhereClauseOperation.GT, ["A"]),
                ds.find_field(title="Rank").filter(WhereClauseOperation.LT, ["7"]),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) == 6

    def test_round_2_in_compeng(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Measure Non-Win": "ROUND(SUM([sales]) / 1000.0, 2)",
                "Measure Win": "ROUND(MAX(SUM([sales]) TOTAL) / 1000.0, 2)",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="Measure Non-Win"),
                ds.find_field(title="Measure Win"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) == 1

        non_win = float(data_rows[0][0])
        win = float(data_rows[0][1])

        assert win == pytest.approx(non_win)

    def test_subquery_column_name_conflict(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Group Sales": "SUM([sales])",
                "Sales RSUM BFB": ("RSUM(SUM([sales]) TOTAL BEFORE FILTER BY [order_date])"),
                "Sales RSUM BFB MAVG BFB": (
                    "MAVG(RSUM(SUM([sales]) TOTAL BEFORE FILTER BY [order_date]), 100 BEFORE FILTER BY [order_date])"
                ),
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="Sales RSUM BFB MAVG BFB"),
                ds.find_field(title="Group Sales"),
            ],
            order_by=[
                ds.find_field(title="order_date"),
            ],
            filters=[
                ds.find_field(title="order_date").filter(
                    op=WhereClauseOperation.EQ,
                    values=["2014-05-01"],
                ),
                ds.find_field(title="Sales RSUM BFB").filter(
                    op=WhereClauseOperation.LT,
                    values=["2000000"],
                ),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json

    def test_bfb_with_only_winfunc_measure(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Sales SUM": "SUM([sales])",
                "Sales RSUM BFB": "RSUM(SUM([sales]) TOTAL ORDER BY [order_date] BEFORE FILTER BY [order_date])",
            },
        )

        def get_data(measures: tuple[str, ...]) -> list[list[str]]:
            result_resp = data_api.get_result(
                dataset=ds,
                fields=[ds.find_field(title="order_date"), *[ds.find_field(title=measure) for measure in measures]],
                filters=[
                    ds.find_field(title="category").filter(WhereClauseOperation.EQ, ["Office Supplies"]),
                    ds.find_field(title="order_date").filter(WhereClauseOperation.GT, ["2014-04-01"]),
                ],
                fail_ok=True,
            )
            assert result_resp.status_code == HTTPStatus.OK, result_resp.json
            data_rows = get_data_rows(result_resp)
            assert len(data_rows) > 1
            return data_rows

        data_rows_no_sum = get_data(measures=("Sales RSUM BFB",))
        data_rows_with_sum = get_data(measures=("Sales RSUM BFB", "Sales SUM"))
        assert len(data_rows_no_sum) == len(data_rows_with_sum)
        for row_no_sum, row_with_sum in zip(data_rows_no_sum, data_rows_with_sum, strict=True):
            assert row_no_sum[0] == row_with_sum[0]  # The dimension
            assert row_no_sum[1] == row_with_sum[1]  # The measure
