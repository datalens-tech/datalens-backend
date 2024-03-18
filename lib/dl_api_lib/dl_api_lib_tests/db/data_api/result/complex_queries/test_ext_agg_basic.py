from collections import defaultdict
import datetime
from http import HTTPStatus
from typing import (
    Any,
    Iterable,
)

import pytest

from dl_api_client.dsmaker.primitives import Dataset
from dl_api_client.dsmaker.shortcuts.dataset import add_formulas_to_dataset
from dl_api_client.dsmaker.shortcuts.result_data import (
    get_data_rows,
    get_regular_result_data,
)
from dl_api_lib_testing.connector.complex_queries import DefaultBasicExtAggregationTestSuite
from dl_api_lib_testing.helpers.data_source import data_source_settings_from_table
from dl_api_lib_tests.db.base import DefaultApiTestBase
from dl_constants.enums import (
    UserDataType,
    WhereClauseOperation,
)
from dl_core_testing.database import (
    C,
    make_table,
)
from dl_query_processing.compilation.primitives import CompiledMultiQueryBase
from dl_query_processing.translation.multi_level_translator import MultiLevelQueryTranslator


class TestBasicExtendedAggregations(DefaultApiTestBase, DefaultBasicExtAggregationTestSuite):
    # Add to the default set of tests

    def test_fixed_same_as_exclude(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "sales sum": "SUM([sales])",
                "sales sum fx city": "SUM([sales] FIXED [city])",
                "sales sum exc category": "SUM([sales] EXCLUDE [category])",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="city"),
                ds.find_field(title="category"),
                ds.find_field(title="sales sum"),
                ds.find_field(title="sales sum fx city"),
                ds.find_field(title="sales sum exc category"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)

        for row in data_rows:
            assert float(row[3]) == float(row[4])

    def test_nested_lod(self, control_api, data_api, saved_dataset):
        """
        Check nested LODs.

        Calculate the per-city average per-category sum of sales
        (first the dimensions expand to allow `category` in, and then collapse
        to the request default of just `city`)
        """
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "sales sum": "SUM([sales])",
                "sales sum inc category": "SUM([sales] INCLUDE [category])",
                "nested LOD AVG": "AVG([sales sum inc category])",
                "nested LOD SUM": "SUM([sales sum inc category])",
            },
        )

        def get_expected_data() -> tuple[dict[str, float], dict[str, float]]:
            # 1. Get ungrouped data
            ungrouped_result_resp = data_api.get_result(
                dataset=ds,
                fields=[
                    ds.find_field(title="city"),
                    ds.find_field(title="category"),
                    ds.find_field(title="sales"),
                ],
                disable_group_by=True,
                fail_ok=True,
            )
            assert ungrouped_result_resp.status_code == HTTPStatus.OK, ungrouped_result_resp.json

            # 2. Group data by city and category and calculate the sum of sales per group
            data_rows = get_data_rows(ungrouped_result_resp)
            sum_by_city_and_cat: dict[tuple[str, str], float] = defaultdict(lambda: 0)
            for row in data_rows:
                sum_by_city_and_cat[(row[0], row[1])] += float(row[2])
            sum_list_by_city: dict[str, list[float]] = defaultdict(list)
            for (city, _cat), sum_value in sum_by_city_and_cat.items():
                sum_list_by_city[city].append(sum_value)

            # 3. Group by city and calculate average of sales sums
            avg_sum_by_city = {city: sum(sum_list) / len(sum_list) for city, sum_list in sum_list_by_city.items()}
            sum_sum_by_city = {city: sum(sum_list) for city, sum_list in sum_list_by_city.items()}
            return avg_sum_by_city, sum_sum_by_city

        # Calculate expected results
        exp_avg_by_city, exp_sum_by_city = get_expected_data()
        # Get actual results
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="city"),
                # ds.find_field(title='sales sum'),
                ds.find_field(title="nested LOD AVG"),
                ds.find_field(title="nested LOD SUM"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json

        # Compare values for each `city`
        data_rows = get_data_rows(result_resp)
        for row in data_rows:
            city = row[0]
            city_sum_avg = float(row[1])
            # Use approx here because DB and Python float calculations do not match exactly
            assert city_sum_avg == pytest.approx(exp_avg_by_city[city])
            city_sum_sum = float(row[2])
            assert city_sum_sum == pytest.approx(exp_sum_by_city[city])

    def test_lod_nested_zero_dim_aggregation_with_filter(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "sales sum": "SUM([sales])",
                "sales sum total": "SUM(SUM([sales] INCLUDE [city]))",
            },
        )

        def get_total_value() -> float:
            total_sum_result = data_api.get_result(
                dataset=ds,
                fields=[ds.find_field(title="sales sum")],
                filters=[
                    ds.find_field(title="category").filter(
                        WhereClauseOperation.EQ,
                        values=["Office Supplies"],
                    ),
                ],
            )
            data_rows = get_data_rows(total_sum_result)
            return float(data_rows[0][0])

        expected_total_value = get_total_value()

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="sales sum total"),
            ],
            filters=[
                ds.find_field(title="category").filter(
                    WhereClauseOperation.EQ,
                    values=["Office Supplies"],
                ),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)

        for row_idx, row in enumerate(data_rows):
            assert float(row[0]) == expected_total_value, f"total sum doesn't match expected number in row {row_idx}"

    def test_double_agg_ratio(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "sales sum": "SUM([sales])",
                "simple ratio": "SUM([sales]) / SUM([sales] FIXED)",
                "double agg ratio": "SUM([sales]) / SUM(SUM([sales]) FIXED)",
            },
        )

        def get_total_value() -> float:
            total_sum_result = data_api.get_result(
                dataset=ds,
                fields=[ds.find_field(title="sales sum")],
            )
            data_rows = get_data_rows(total_sum_result)
            return float(data_rows[0][0])

        expected_total_value = get_total_value()

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="category"),
                ds.find_field(title="sales sum"),
                ds.find_field(title="simple ratio"),
                ds.find_field(title="double agg ratio"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)

        for row_idx, row in enumerate(data_rows):
            expected_value = float(row[1]) / expected_total_value
            assert (
                float(row[3]) == float(row[2]) == expected_value
            ), f"total sum doesn't match expected number in row {row_idx}"

    def test_nested_zero_dim_aggregation(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "sales sum": "SUM([sales])",
                "city avg sales sum": "AVG(SUM([sales] INCLUDE [city]))",
            },
        )

        def get_expected_value() -> float:
            # 1. Get per-city data
            result_resp = data_api.get_result(
                dataset=ds,
                fields=[
                    ds.find_field(title="city"),
                    ds.find_field(title="sales sum"),
                ],
                fail_ok=True,
            )
            assert result_resp.status_code == HTTPStatus.OK, result_resp.json
            data_rows = get_data_rows(result_resp)
            # Calculate the average
            return sum(float(row[1]) for row in data_rows) / len(data_rows)

        expected_avg_value = get_expected_value()

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="city avg sales sum"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) == 1
        assert float(data_rows[0][0]) == pytest.approx(expected_avg_value)

    def test_lod_with_bfb(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "sales sum": "SUM([sales])",
                "sales sum fx Date": "SUM([sales] FIXED [order_date])",
                "sales sum fx Date BFB": "SUM([sales] FIXED [order_date] BEFORE FILTER BY [category])",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="category"),
                ds.find_field(title="sales sum"),
                ds.find_field(title="sales sum fx Date"),
                ds.find_field(title="sales sum fx Date BFB"),
            ],
            filters=[
                ds.find_field(title="category").filter(
                    WhereClauseOperation.EQ,
                    values=["Office Supplies"],
                ),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)

        sales_by_date = defaultdict(lambda: 0)
        lod_sales_by_date = {}
        case_1_cnt = 0
        case_2_cnt = 0
        for row in data_rows:
            sales_by_date[row[0]] += float(row[2])
            lod_sales_by_date[row[0]] = float(row[4])
            # BFB sum is equal to non-BFB sum only when there is exactly one category value,
            # so it also equals the regular per-row sum
            if float(row[4]) == float(row[3]):
                assert float(row[4]) == float(row[3]) == float(row[2])
                case_1_cnt += 1
            else:  # Otherwise BFB is greater than non-BFB
                assert float(row[4]) > float(row[3])
                case_2_cnt += 1

        assert lod_sales_by_date == pytest.approx(lod_sales_by_date)
        assert case_1_cnt > 0
        assert case_2_cnt > 0

    def test_workaround_for_inconsistent_agg(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "My Field": "SUM(SUM([sales] INCLUDE [Order ID]) / SUM([sales] FIXED))",
            },
        )
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="My Field"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) == 1
        assert pytest.approx(float(data_rows[0][0])) == 1

    def test_lod_nested_multiple_times(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                # Plain and simple row count
                "Agg 1": "COUNT([sales])",
                # All the others are pretty much the same thing, but with multiple aggregation levels
                "Agg 2": "SUM(COUNT([sales] INCLUDE [city]))",
                "Agg 3": "SUM(SUM(COUNT([sales] INCLUDE [city]) INCLUDE [category]))",
                "Agg 4": "SUM(SUM(SUM(COUNT([sales] INCLUDE [city]) INCLUDE [category]) INCLUDE [order_date]))",
                "Agg 5": (
                    "SUM(SUM(SUM(SUM(COUNT([sales] INCLUDE [city]) INCLUDE [category]) "
                    "INCLUDE [order_date]) INCLUDE [region]))"
                ),
            },
        )

        def get_single_row_data(field_names: Iterable[str]) -> tuple[int, ...]:
            result_resp = data_api.get_result(
                dataset=ds,
                fail_ok=True,
                fields=[ds.find_field(title=field_name) for field_name in field_names],
            )
            assert result_resp.status_code == HTTPStatus.OK, result_resp.json
            data_rows = get_data_rows(result_resp)
            assert len(data_rows) == 1, "There must be exactly 1 row of data"
            return tuple(int(item) for item in data_rows[0])

        value_1 = get_single_row_data(["Agg 1"])[0]
        value_2 = get_single_row_data(["Agg 2"])[0]
        assert value_2 == value_1
        value_3 = get_single_row_data(["Agg 3"])[0]
        assert value_3 == value_1
        value_4 = get_single_row_data(["Agg 4"])[0]
        assert value_4 == value_1
        value_5 = get_single_row_data(["Agg 4"])[0]
        assert value_5 == value_1

        # Check the two at a time with the order of aggregation increasing by 1
        simultaneous_values = get_single_row_data(["Agg 1", "Agg 2"])
        assert len(set(simultaneous_values)) == 1
        assert next(iter(simultaneous_values)) == value_1

        simultaneous_values = get_single_row_data(["Agg 2", "Agg 3"])
        assert len(set(simultaneous_values)) == 1
        assert next(iter(simultaneous_values)) == value_1

        simultaneous_values = get_single_row_data(["Agg 3", "Agg 4"])
        assert len(set(simultaneous_values)) == 1
        assert next(iter(simultaneous_values)) == value_1

        simultaneous_values = get_single_row_data(["Agg 4", "Agg 5"])
        assert len(set(simultaneous_values)) == 1
        assert next(iter(simultaneous_values)) == value_1

        # Check with diffs 2, 3 and 4
        simultaneous_values = get_single_row_data(["Agg 1", "Agg 3"])
        assert len(set(simultaneous_values)) == 1
        assert next(iter(simultaneous_values)) == value_1

        simultaneous_values = get_single_row_data(["Agg 1", "Agg 4"])
        assert len(set(simultaneous_values)) == 1
        assert next(iter(simultaneous_values)) == value_1

        simultaneous_values = get_single_row_data(["Agg 1", "Agg 5"])
        assert len(set(simultaneous_values)) == 1
        assert next(iter(simultaneous_values)) == value_1

        # The killer query - do them all at once!
        simultaneous_values = get_single_row_data(["Agg 1", "Agg 2", "Agg 3", "Agg 4", "Agg 5"])
        assert len(set(simultaneous_values)) == 1
        assert next(iter(simultaneous_values)) == value_1

    def test_two_same_dim_lods_in_different_subqueries(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "sales sum": "SUM([sales])",
                "same dim LOD": "SUM([sales] FIXED [city], [category])",
                "by city": "SUM(SUM([same dim LOD] FIXED [city]))",
                "by category": "SUM(SUM([same dim LOD] FIXED [category]))",
            },
        )

        def get_total_value() -> float:
            total_sum_result = data_api.get_result(
                dataset=ds,
                fields=[ds.find_field(title="sales sum")],
            )
            data_rows = get_data_rows(total_sum_result)
            return float(data_rows[0][0])

        total_value = get_total_value()

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="by city"),
                ds.find_field(title="by category"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) == 1
        by_city, by_category = float(data_rows[0][0]), float(data_rows[0][1])
        assert by_city == pytest.approx(by_category) == pytest.approx(total_value)

    def test_two_total_sums_with_different_nested_includes(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Sales Sum": "SUM([sales])",
                "By City": "SUM(SUM([sales] INCLUDE [city]))",
                "By Category": "SUM(SUM([sales] INCLUDE [category]))",
            },
        )

        def get_total_value() -> float:
            total_sum_result = data_api.get_result(
                dataset=ds,
                fields=[ds.find_field(title="Sales Sum")],
            )
            data_rows = get_data_rows(total_sum_result)
            return float(data_rows[0][0])

        total_value = get_total_value()

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="By City"),
                ds.find_field(title="By Category"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) == 1
        by_city, by_category = float(data_rows[0][0]), float(data_rows[0][1])
        assert by_city == pytest.approx(by_category) == pytest.approx(total_value)

    def test_agg_with_lod_over_window_function(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Lod Over Win 1": "MIN(RANK_UNIQUE(SUM([sales] INCLUDE [city]) TOTAL) FIXED)",
                "Lod Over Win 2": "MAX(RANK_UNIQUE(SUM([sales] INCLUDE [city]) TOTAL) FIXED)",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="city"),
                ds.find_field(title="Lod Over Win 1"),
                ds.find_field(title="Lod Over Win 2"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) > 10  # There should be a lot of them
        for row in data_rows:
            assert int(row[1]) == 1
            assert int(row[2]) == len(data_rows)

    def test_lod_compatibility_error(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            exp_status=HTTPStatus.BAD_REQUEST,
            formulas={
                "Invalid Field": "SUM(AVG([sales] INCLUDE [city]) - AVG([sales] INCLUDE [category]))",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="Invalid Field"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.BAD_REQUEST, result_resp.json
        assert result_resp.bi_status_code == "ERR.DS_API.FORMULA.VALIDATION.LOD.INCOMPATIBLE_DIMENSIONS"

    def test_dimension_with_single_and_double_agg(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Single Agg": "SUM([sales])",
                "Double Agg": "SUM(SUM([sales] INCLUDE [city]))",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="Single Agg"),
            ],
        )
        data_rows = get_data_rows(result_resp)
        expected_by_date = {row[0]: float(row[1]) for row in data_rows}

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="Single Agg"),
                ds.find_field(title="Double Agg"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) == len(expected_by_date)
        for row in data_rows:
            date_str, single_agg_value, double_agg_value = row[0], float(row[1]), float(row[2])
            assert single_agg_value == pytest.approx(double_agg_value) == pytest.approx(expected_by_date[date_str])

    def test_lod_with_ago(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Sales Sum": "SUM([sales])",
                "Lod With Ago": """
                AVG(
                    SUM(  /* Doesn't really aggregate anything
                           * because it has the same dims as the nested SUMs */
                        SUM([sales]) - ZN(AGO(SUM([sales]), [order_date], "day"))
                        INCLUDE [order_date]
                    )
                )
            """,
            },
        )

        def get_expected_value() -> float:
            result_resp = data_api.get_result(
                dataset=ds,
                fields=[
                    ds.find_field(title="order_date"),
                    ds.find_field(title="Sales Sum"),
                ],
            )
            data_rows = get_data_rows(result_resp)
            sales_by_date = {row[0]: float(row[1]) for row in data_rows}

            total_sum = 0.0
            for date_str in sorted(sales_by_date):
                ago_date_str = str(datetime.date.fromisoformat(date_str) - datetime.timedelta(days=1))
                sales_value = sales_by_date[date_str]
                ago_sales_value = sales_by_date.get(ago_date_str, 0)
                total_sum += sales_value - ago_sales_value

            return total_sum / len(sales_by_date)

        expected_value = get_expected_value()

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="Lod With Ago"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) == 1
        actual_value = float(data_rows[0][0])
        assert actual_value == pytest.approx(expected_value)

    def test_lod_with_ago_and_constant_field(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Sales Sum": "SUM([sales])",
                "Sales LOD": "SUM([Sales Sum] WITHIN [order_date])",
                "Sales AGO": "AGO([Sales Sum], [order_date], 1)",
                "Constant Field": '"lol"',
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="Sales LOD"),
                ds.find_field(title="Sales AGO"),
                ds.find_field(title="Constant Field"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert data_rows
        assert all(row[-1] == "lol" for row in data_rows)

    def test_toplevel_lod_extra_dimension_error(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Extra Dim Field": "AVG([sales] INCLUDE [category])",
                "Missing Dim Field": "AVG([sales] FIXED)",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="Extra Dim Field"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.BAD_REQUEST, result_resp.json
        assert result_resp.bi_status_code == "ERR.DS_API.FORMULA.VALIDATION.LOD.INVALID_TOPLEVEL_DIMENSIONS"

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="Missing Dim Field"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json

    def test_double_agg_no_lod(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Double Agg": "COUNT(COUNT([sales]))",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="Double Agg"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)

        assert len(data_rows) == 1
        assert int(data_rows[0][0]) == 1

    def test_bfb_no_lod(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Sales Sum": "SUM([sales])",
                "Bfb No Lod": "SUM([sales] BEFORE FILTER BY [category])",
            },
        )

        result_resp = data_api.get_result(dataset=ds, fields=[ds.find_field(title="Sales Sum")])
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) == 1
        expected_total_value = float(data_rows[0][0])

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="Bfb No Lod"),
            ],
            filters=[
                ds.find_field(title="category").filter(op=WhereClauseOperation.EQ, values=["Office Supplies"]),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)

        assert len(data_rows) == 1
        assert float(data_rows[0][0]) == expected_total_value

    def test_regular_agg_with_bfb_agg(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Sales Sum": "SUM([sales])",
                "Agg Bfb": "SUM([sales] EXCLUDE [category] BEFORE FILTER BY [category])",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="region"),
                ds.find_field(title="category"),
                ds.find_field(title="Sales Sum"),
            ],
            filters=[
                ds.find_field(title="category").filter(WhereClauseOperation.EQ, ["Office Supplies"]),
            ],
        )
        data_rows = get_data_rows(result_resp)
        expected_by_region = {row[0]: float(row[2]) for row in data_rows}

        # Same request, but with `Agg Bfb`
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="region"),
                ds.find_field(title="category"),
                ds.find_field(title="Sales Sum"),
                ds.find_field(title="Agg Bfb"),
            ],
            filters=[
                ds.find_field(title="category").filter(WhereClauseOperation.EQ, ["Office Supplies"]),
            ],
        )
        data_rows = get_data_rows(result_resp)
        actual_by_region = {row[0]: float(row[2]) for row in data_rows}

        assert actual_by_region == expected_by_region

    def test_double_aggregation_optimization(self, control_api, data_api, saved_dataset, monkeypatch):
        """
        Test double aggregation optimizations in formulas like `SUM(SUM([sales]))` (-> `SUM([sales])`)
        """
        data_container: dict[str, Any] = {}
        dataset_id = saved_dataset.id

        def _log_query_complexity_stats(self, compiled_multi_query: CompiledMultiQueryBase) -> None:
            data_container.update(
                {
                    "query_count": compiled_multi_query.query_count(),
                }
            )

        # Capture query complexity info
        monkeypatch.setattr(MultiLevelQueryTranslator, "_log_query_complexity_stats", _log_query_complexity_stats)

        def check_agg(first_agg_name: str, second_agg_name: str) -> None:
            ds = add_formulas_to_dataset(
                api_v1=control_api,
                dataset_id=dataset_id,
                formulas={
                    f"{first_agg_name} {second_agg_name} Opt": f"{second_agg_name}({first_agg_name}([sales]))",
                    f"{first_agg_name} {second_agg_name}": f"{second_agg_name}({first_agg_name}([sales])+0)",
                },
            )

            data = get_regular_result_data(
                ds,
                data_api,
                field_names=["category", f"{first_agg_name} {second_agg_name}"],
            )[f"{first_agg_name} {second_agg_name}"]
            assert data_container["query_count"] >= 3
            data_opt = get_regular_result_data(
                ds,
                data_api,
                field_names=["category", f"{first_agg_name} {second_agg_name} Opt"],
            )[f"{first_agg_name} {second_agg_name} Opt"]
            assert data_container["query_count"] == 1
            assert sorted([float(val) for val in data_opt]) == pytest.approx(
                sorted([float(val) for val in data])
            ), f"Got different values for {first_agg_name}({second_agg_name}(...))"

        for first_agg_name in ("avg", "count"):
            for second_agg_name in ("sum", "any", "max", "min", "count", "countd"):
                # no optimization for AVG
                check_agg(first_agg_name.upper(), second_agg_name.upper())

    def test_lod_fixed_markup(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Bold city": "BOLD([city])",
                "Sales Sum fx": "AVG(SUM([sales] FIXED))",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="Bold city"),
                ds.find_field(title="Sales Sum fx"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json

    def test_bug_bi_3425_deeply_nested_bfb(self, control_api, data_api, db, saved_connection_id):
        raw_data = [
            {"id": 10, "city": "New York", "category": "Office Supplies", "sales": 1},
            {"id": 11, "city": "New York", "category": "Office Supplies", "sales": 10},
            {"id": 12, "city": "New York", "category": "Furniture", "sales": 100},
            {"id": 13, "city": "New York", "category": "Furniture", "sales": 1},
            {"id": 14, "city": "New Rochelle", "category": "Office Supplies", "sales": 10000},
            {"id": 15, "city": "New Rochelle", "category": "Office Supplies", "sales": 100000},
            {"id": 16, "city": "New Rochelle", "category": "Furniture", "sales": 10000},
            {"id": 17, "city": "New Rochelle", "category": "Furniture", "sales": 10000000},
            {"id": 18, "city": "Detroit", "category": "Office Supplies", "sales": 1},
            {"id": 19, "city": "Detroit", "category": "Office Supplies", "sales": 100},
            {"id": 20, "city": "Detroit", "category": "Furniture", "sales": 10000},
            {"id": 21, "city": "Detroit", "category": "Furniture", "sales": 1000000},
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
                "city_1st": 'GET_ITEM(SPLIT([city], " "), 1)',
                "sales_sum_bfb": "SUM(SUM([sales]) EXCLUDE [city] BEFORE FILTER BY [city_1st])",
                "sales_sum_if": 'SUM_IF([sales], [city_1st] = "New")',
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="city"),
                ds.find_field(title="sales_sum_if"),
                ds.find_field(title="sales_sum_bfb"),
            ],
            order_by=[
                ds.find_field(title="city"),
            ],
            filters=[
                ds.find_field(title="city_1st").filter(WhereClauseOperation.EQ, ["New"]),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows_1 = get_data_rows(result_resp)
        data_rows_1_stripped = [row[:2] for row in data_rows_1]  # Strip off `sales_sum_bfb` values

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="city"),
                ds.find_field(title="sales_sum_if"),
                # This time without `sales_sum_bfb`
            ],
            order_by=[
                ds.find_field(title="city"),
            ],
            filters=[
                ds.find_field(title="city_1st").filter(WhereClauseOperation.EQ, ["New"]),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows_2 = get_data_rows(result_resp)

        assert data_rows_1_stripped == data_rows_2

    def test_lod_only_in_filter(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Sales Sum": "SUM([sales])",
                "Daily Profit Sum": "AVG(SUM([sales] INCLUDE [order_date]))",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="city"),
                ds.find_field(title="Sales Sum"),
            ],
            filters=[
                ds.find_field(title="Daily Profit Sum").filter(WhereClauseOperation.GT, ["10.0"]),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) > 0

    def test_lod_in_filter_and_select(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Daily Profit Sum": "AVG(SUM([sales] INCLUDE [order_date]))",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="city"),
                ds.find_field(title="Daily Profit Sum"),
            ],
            filters=[
                ds.find_field(title="Daily Profit Sum").filter(WhereClauseOperation.GT, ["10.0"]),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) > 0

    def test_replace_original_dim_with_another(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Measure": "AVG(SUM([sales] FIXED [city]))",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="category"),
                ds.find_field(title="Measure"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert len(data_rows) == 3  # There are 3 categories

    def test_bi_4534_inconsistent_aggregation(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Sum": "SUM([sales])",
                "Measure": "SUM([Sum]) - SUM(AGO([Sum], [order_date]))",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="Measure"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK

    def test_bi_4652_measure_filter_with_total_in_select(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Measure": "SUM([sales])",
                "Total Measure": "SUM([sales] FIXED)",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="city"),
                ds.find_field(title="Total Measure"),
            ],
            filters=[
                ds.find_field(title="Measure").filter(WhereClauseOperation.GT, ["10.0"]),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)
        dim_values = [row[0] for row in data_rows]
        assert len(dim_values) == len(set(dim_values)), "Dimension values are not unique"

    @pytest.mark.xfail(reason="https://github.com/datalens-tech/datalens-backend/issues/98")  # FIXME
    def test_fixed_with_unknown_field(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "sales sum fx unknown": "SUM([sales] FIXED [unknown])",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="sales sum fx unknown"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.BAD_REQUEST, result_resp.json
