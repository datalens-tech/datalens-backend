from __future__ import annotations

from collections import defaultdict
import datetime
from http import HTTPStatus
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Tuple,
)

import pytest

from bi_legacy_test_bundle_tests.api_lib.utils import data_source_settings_from_table
from dl_api_client.dsmaker.primitives import (
    Dataset,
    ResultField,
)
from dl_api_client.dsmaker.shortcuts.dataset import (
    add_formulas_to_dataset,
    create_basic_dataset,
)
from dl_api_client.dsmaker.shortcuts.result_data import (
    get_data_rows,
    get_regular_result_data,
)
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

from bi_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL


def test_lod_fixed_single_dim_in_two_dim_query(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Sales Sum": "SUM([Sales])",
            "Sales Sum fx City": "SUM([Sales] FIXED [City])",
            "Sales Sum fx Category": "SUM([Sales] FIXED [Category])",
        },
    )

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="City"),
            ds.find_field(title="Category"),
            ds.find_field(title="Sales Sum"),
            ds.find_field(title="Sales Sum fx City"),
            ds.find_field(title="Sales Sum fx Category"),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)

    sum_by_city = defaultdict(lambda: 0)
    for row in data_rows:
        sum_by_city[row[0]] += float(row[2])
    sum_by_category = defaultdict(lambda: 0)
    for row in data_rows:
        sum_by_category[row[1]] += float(row[2])

    for row in data_rows:
        assert float(row[3]) == sum_by_city[row[0]]
        assert float(row[4]) == sum_by_category[row[1]]


def test_fixed_same_as_exclude(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Sales Sum": "SUM([Sales])",
            "Sales Sum fx City": "SUM([Sales] FIXED [City])",
            "Sales Sum exc Category": "SUM([Sales] EXCLUDE [Category])",
        },
    )

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="City"),
            ds.find_field(title="Category"),
            ds.find_field(title="Sales Sum"),
            ds.find_field(title="Sales Sum fx City"),
            ds.find_field(title="Sales Sum exc Category"),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)

    for row in data_rows:
        assert float(row[3]) == float(row[4])


def test_nested_lod(api_v1, data_api_v2, dataset_id):
    """
    Check nested LODs.

    Calculate the per-City average per-Category sum of Sales
    (first the dimensions expand to allow `Category` in, and then collapse
    to the request default of just `City`)
    """
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Sales Sum": "SUM([Sales])",
            "Sales Sum inc Category": "SUM([Sales] INCLUDE [Category])",
            "Nested LOD AVG": "AVG([Sales Sum inc Category])",
            "Nested LOD SUM": "SUM([Sales Sum inc Category])",
        },
    )

    def get_expected_data() -> Tuple[Dict[str, float], Dict[str, float]]:
        # 1. Get ungrouped data
        ungrouped_result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="City"),
                ds.find_field(title="Category"),
                ds.find_field(title="Sales"),
            ],
            disable_group_by=True,
            fail_ok=True,
        )
        assert ungrouped_result_resp.status_code == HTTPStatus.OK, ungrouped_result_resp.json

        # 2. Group data by City and Category and calculate the sum of Sales per group
        data_rows = get_data_rows(ungrouped_result_resp)
        sum_by_city_and_cat: Dict[Tuple[str, str], float] = defaultdict(lambda: 0)
        for row in data_rows:
            sum_by_city_and_cat[(row[0], row[1])] += float(row[2])
        sum_list_by_city: Dict[str, List[float]] = defaultdict(list)
        for (city, cat), sum_value in sum_by_city_and_cat.items():
            sum_list_by_city[city].append(sum_value)

        # 3. Group by City and calculate average of Sales sums
        avg_sum_by_city = {city: sum(sum_list) / len(sum_list) for city, sum_list in sum_list_by_city.items()}
        sum_sum_by_city = {city: sum(sum_list) for city, sum_list in sum_list_by_city.items()}
        return avg_sum_by_city, sum_sum_by_city

    # Calculate expected results
    exp_avg_by_city, exp_sum_by_city = get_expected_data()
    # Get actual results
    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="City"),
            # ds.find_field(title='Sales Sum'),
            ds.find_field(title="Nested LOD AVG"),
            ds.find_field(title="Nested LOD SUM"),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json

    # Compare values for each `City`
    data_rows = get_data_rows(result_resp)
    for row in data_rows:
        city = row[0]
        city_sum_avg = float(row[1])
        # Use approx here because DB and Python float calculations do not match exactly
        assert city_sum_avg == pytest.approx(exp_avg_by_city[city])
        city_sum_sum = float(row[2])
        assert city_sum_sum == pytest.approx(exp_sum_by_city[city])


def test_lod_zero_dim_aggregation(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Sales Sum": "SUM([Sales])",
            "Sales Sum Total": "SUM([Sales] FIXED)",
        },
    )

    def get_total_value() -> float:
        total_sum_result = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title="Sales Sum")],
        )
        data_rows = get_data_rows(total_sum_result)
        return float(data_rows[0][0])

    expected_total_value = get_total_value()

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="City"),
            ds.find_field(title="Sales Sum"),
            ds.find_field(title="Sales Sum Total"),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)

    total_sum = sum(float(row[1]) for row in data_rows)

    for row_idx, row in enumerate(data_rows):
        assert (
            float(row[2]) == total_sum == expected_total_value
        ), f"Total sum doesn't match expected number in row {row_idx}"


def test_lod_nested_zero_dim_aggregation_with_filter(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Sales Sum": "SUM([Sales])",
            "Sales Sum Total": "SUM(SUM([Sales] INCLUDE [City]))",
        },
    )

    def get_total_value() -> float:
        total_sum_result = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title="Sales Sum")],
            filters=[
                ds.find_field(title="Category").filter(
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
            ds.find_field(title="Sales Sum Total"),
        ],
        filters=[
            ds.find_field(title="Category").filter(
                WhereClauseOperation.EQ,
                values=["Office Supplies"],
            ),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)

    for row_idx, row in enumerate(data_rows):
        assert float(row[0]) == expected_total_value, f"Total sum doesn't match expected number in row {row_idx}"


def test_double_agg_ratio(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Sales Sum": "SUM([Sales])",
            "Simple Ratio": "SUM([Sales]) / SUM([Sales] FIXED)",
            "Double Agg Ratio": "SUM([Sales]) / SUM(SUM([Sales]) FIXED)",
        },
    )

    def get_total_value() -> float:
        total_sum_result = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title="Sales Sum")],
        )
        data_rows = get_data_rows(total_sum_result)
        return float(data_rows[0][0])

    expected_total_value = get_total_value()

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="Category"),
            ds.find_field(title="Sales Sum"),
            ds.find_field(title="Simple Ratio"),
            ds.find_field(title="Double Agg Ratio"),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)

    for row_idx, row in enumerate(data_rows):
        expected_value = float(row[1]) / expected_total_value
        assert (
            float(row[3]) == float(row[2]) == expected_value
        ), f"Total sum doesn't match expected number in row {row_idx}"


def test_nested_zero_dim_aggregation(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Sales Sum": "SUM([Sales])",
            "City Avg Sales Sum": "AVG(SUM([Sales] INCLUDE [City]))",
        },
    )

    def get_expected_value() -> float:
        # 1. Get per-city data
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="City"),
                ds.find_field(title="Sales Sum"),
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
            ds.find_field(title="City Avg Sales Sum"),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)
    assert len(data_rows) == 1
    assert float(data_rows[0][0]) == pytest.approx(expected_avg_value)


def test_lod_with_bfb(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Sales Sum": "SUM([Sales])",
            "Sales Sum fx Date": "SUM([Sales] FIXED [Order Date])",
            "Sales Sum fx Date BFB": "SUM([Sales] FIXED [Order Date] BEFORE FILTER BY [Category])",
        },
    )

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="Order Date"),
            ds.find_field(title="Category"),
            ds.find_field(title="Sales Sum"),
            ds.find_field(title="Sales Sum fx Date"),
            ds.find_field(title="Sales Sum fx Date BFB"),
        ],
        filters=[
            ds.find_field(title="Category").filter(
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
        # BFB sum is equal to non-BFB sum only when there is exactly one Category value,
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


def test_workaround_for_inconsistent_agg(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "My Field": "SUM(SUM([Sales] INCLUDE [Order ID]) / SUM([Sales] FIXED))",
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


def test_lod_nested_multiple_times(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            # Plain and simple row count
            "Agg 1": "COUNT([Sales])",
            # All the others are pretty much the same thing, but with multiple aggregation levels
            "Agg 2": "SUM(COUNT([Sales] INCLUDE [City]))",
            "Agg 3": "SUM(SUM(COUNT([Sales] INCLUDE [City]) INCLUDE [Category]))",
            "Agg 4": "SUM(SUM(SUM(COUNT([Sales] INCLUDE [City]) INCLUDE [Category]) INCLUDE [Order Date]))",
            "Agg 5": (
                "SUM(SUM(SUM(SUM(COUNT([Sales] INCLUDE [City]) INCLUDE [Category]) "
                "INCLUDE [Order Date]) INCLUDE [Region]))"
            ),
        },
    )

    def get_single_row_data(field_names: Iterable[str]) -> Tuple[int, ...]:
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


def test_any_db_total_lod(
    api_v1,
    data_api_v2,
    any_db_saved_connection,
    any_db_table,
):
    data_api = data_api_v2
    ds = create_basic_dataset(
        api_v1=api_v1,
        connection_id=any_db_saved_connection.uuid,
        data_source_settings=data_source_settings_from_table(table=any_db_table),
        formulas={
            "div 2": "DIV([int_value], 2)",
            "div 3": "DIV([int_value], 3)",
            "Agg 1": "SUM([int_value])",
            "Agg 2": "SUM(SUM([int_value] INCLUDE [div 2]))",
            "Agg 3": "SUM(SUM(SUM([int_value] INCLUDE [div 2]) INCLUDE [div 3]))",
        },
    )

    def get_single_row_data(field_names: Iterable[str]) -> Tuple[int, ...]:
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

    def check_equality_of_totals(*field_names: str) -> None:
        simultaneous_values = get_single_row_data(field_names)
        assert len(set(simultaneous_values)) == 1
        assert next(iter(simultaneous_values)) == value_1

    if any_db_saved_connection.conn_type != CONNECTION_TYPE_MSSQL:
        # FIXME: Fix for MSSQL
        check_equality_of_totals("Agg 1", "Agg 2")
        check_equality_of_totals("Agg 2", "Agg 3")
        check_equality_of_totals("Agg 1", "Agg 3")
        check_equality_of_totals("Agg 1", "Agg 2", "Agg 3")


def test_two_same_dim_lods_in_different_subqueries(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Sales Sum": "SUM([Sales])",
            "Same Dim LOD": "SUM([Sales] FIXED [City], [Category])",
            "By City": "SUM(SUM([Same Dim LOD] FIXED [City]))",
            "By Category": "SUM(SUM([Same Dim LOD] FIXED [Category]))",
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
    assert by_city == by_category == total_value


def test_two_total_sums_with_different_nested_includes(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Sales Sum": "SUM([Sales])",
            "By City": "SUM(SUM([Sales] INCLUDE [City]))",
            "By Category": "SUM(SUM([Sales] INCLUDE [Category]))",
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
    assert by_city == by_category == total_value


def test_agg_with_lod_over_window_function(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Lod Over Win 1": "MIN(RANK_UNIQUE(SUM([Sales] INCLUDE [City]) TOTAL) FIXED)",
            "Lod Over Win 2": "MAX(RANK_UNIQUE(SUM([Sales] INCLUDE [City]) TOTAL) FIXED)",
        },
    )

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="City"),
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


def test_lod_compatibility_error(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        exp_status=HTTPStatus.BAD_REQUEST,
        formulas={
            "Invalid Field": "SUM(AVG([Sales] INCLUDE [City]) - AVG([Sales] INCLUDE [Category]))",
        },
    )

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="Order Date"),
            ds.find_field(title="Invalid Field"),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.BAD_REQUEST, result_resp.json
    assert result_resp.bi_status_code == "ERR.DS_API.FORMULA.VALIDATION.LOD.INCOMPATIBLE_DIMENSIONS"


def test_dimension_with_single_and_double_agg(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Single Agg": "SUM([Sales])",
            "Double Agg": "SUM(SUM([Sales] INCLUDE [City]))",
        },
    )

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="Order Date"),
            ds.find_field(title="Single Agg"),
        ],
    )
    data_rows = get_data_rows(result_resp)
    expected_by_date = {row[0]: float(row[1]) for row in data_rows}

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="Order Date"),
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
        assert single_agg_value == double_agg_value == expected_by_date[date_str]


def test_lod_with_ago(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Sales Sum": "SUM([Sales])",
            "Lod With Ago": """
            AVG(
                SUM(  /* Doesn't really aggregate anything
                       * because it has the same dims as the nested SUMs */
                    SUM([Sales]) - ZN(AGO(SUM([Sales]), [Order Date], "day"))
                    INCLUDE [Order Date]
                )
            )
        """,
        },
    )

    def get_expected_value() -> float:
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="Order Date"),
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
    assert actual_value == expected_value


def test_toplevel_lod_extra_dimension_error(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Extra Dim Field": "AVG([Sales] INCLUDE [Category])",
            "Missing Dim Field": "AVG([Sales] FIXED)",
        },
    )

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="Order Date"),
            ds.find_field(title="Extra Dim Field"),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.BAD_REQUEST, result_resp.json
    assert result_resp.bi_status_code == "ERR.DS_API.FORMULA.VALIDATION.LOD.INVALID_TOPLEVEL_DIMENSIONS"

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="Order Date"),
            ds.find_field(title="Missing Dim Field"),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json


def test_double_agg_no_lod(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Double Agg": "COUNT(COUNT([Sales]))",
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


def test_bfb_no_lod(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Sales Sum": "SUM([Sales])",
            "Bfb No Lod": "SUM([Sales] BEFORE FILTER BY [Category])",
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
            ds.find_field(title="Category").filter(op=WhereClauseOperation.EQ, values=["Office Supplies"]),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)

    assert len(data_rows) == 1
    assert float(data_rows[0][0]) == expected_total_value


def test_regular_agg_with_bfb_agg(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Sales Sum": "SUM([Sales])",
            "Agg Bfb": "SUM([Sales] EXCLUDE [Category] BEFORE FILTER BY [Category])",
        },
    )

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="Region"),
            ds.find_field(title="Category"),
            ds.find_field(title="Sales Sum"),
        ],
        filters=[
            ds.find_field(title="Category").filter(WhereClauseOperation.EQ, ["Office Supplies"]),
        ],
    )
    data_rows = get_data_rows(result_resp)
    expected_by_region = {row[0]: float(row[2]) for row in data_rows}

    # Same request, but with `Agg Bfb`
    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="Region"),
            ds.find_field(title="Category"),
            ds.find_field(title="Sales Sum"),
            ds.find_field(title="Agg Bfb"),
        ],
        filters=[
            ds.find_field(title="Category").filter(WhereClauseOperation.EQ, ["Office Supplies"]),
        ],
    )
    data_rows = get_data_rows(result_resp)
    actual_by_region = {row[0]: float(row[2]) for row in data_rows}

    assert actual_by_region == expected_by_region


def test_double_aggregation_optimization(api_v1, data_api_v2, dataset_id, monkeypatch):
    """
    Test double aggregation optimizations in formulas like `SUM(SUM([Sales]))` (-> `SUM([Sales])`)
    """
    data_api = data_api_v2

    data_container: Dict[str, Any] = {}

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
            api_v1=api_v1,
            dataset_id=dataset_id,
            formulas={
                f"{first_agg_name} {second_agg_name} Opt": f"{second_agg_name}({first_agg_name}([Sales]))",
                f"{first_agg_name} {second_agg_name}": f"{second_agg_name}({first_agg_name}([Sales])+0)",
            },
        )

        data = get_regular_result_data(
            ds,
            data_api,
            field_names=["Category", f"{first_agg_name} {second_agg_name}"],
        )[f"{first_agg_name} {second_agg_name}"]
        assert data_container["query_count"] >= 3
        data_opt = get_regular_result_data(
            ds,
            data_api,
            field_names=["Category", f"{first_agg_name} {second_agg_name} Opt"],
        )[f"{first_agg_name} {second_agg_name} Opt"]
        assert data_container["query_count"] == 1
        assert sorted([float(val) for val in data_opt]) == pytest.approx(
            sorted([float(val) for val in data])
        ), f"Got different values for {first_agg_name}({second_agg_name}(...))"

    for first_agg_name in ("avg", "count"):
        for second_agg_name in ("sum", "any", "max", "min", "count", "countd"):
            # no optimization for AVG
            check_agg(first_agg_name.upper(), second_agg_name.upper())


def test_lod_in_order_by(
    api_v1,
    data_api_v2,
    any_db_saved_connection,
    any_db_table,
):
    if any_db_saved_connection.conn_type == CONNECTION_TYPE_MSSQL:
        # Total LODs don't really work in MSSQL
        pytest.skip()

    data_api = data_api_v2
    ds = create_basic_dataset(
        api_v1=api_v1,
        connection_id=any_db_saved_connection.uuid,
        data_source_settings=data_source_settings_from_table(table=any_db_table),
        formulas={
            "Dimension": "[int_value] % 3",
            "LOD Measure": "SUM([int_value]) / SUM([int_value] FIXED)",
        },
    )

    def get_data(order_by: List[ResultField]) -> list:
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="Dimension"),
                ds.find_field(title="LOD Measure"),
            ],
            order_by=order_by,
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        return get_data_rows(result_resp)

    data_rows = get_data(order_by=[])
    ordered_data_rows = get_data(
        order_by=[
            ds.find_field(title="LOD Measure"),
            ds.find_field(title="Dimension"),
        ]
    )

    data_rows.sort(key=lambda row: (float(row[1]), row[0]))  # (LOD Measure, City)

    assert ordered_data_rows == data_rows


def test_lod_fixed_markup(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Bold city": "BOLD([City])",
            "Sales Sum fx": "AVG(SUM([Sales] FIXED))",
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


def test_bug_bi_3425(api_v1, data_api_v2, clickhouse_db, connection_id):
    data_api = data_api_v2
    db = clickhouse_db

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
    ds.sources["source_1"] = ds.source(connection_id=connection_id, **data_source_settings_from_table(db_table))
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()

    ds = add_formulas_to_dataset(
        api_v1=api_v1,
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


def test_null_dimensions(api_v1, data_api_v2, any_db, any_db_saved_connection):
    data_api = data_api_v2
    db = any_db
    connection_id = any_db_saved_connection.uuid

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
    ds.sources["source_1"] = ds.source(connection_id=connection_id, **data_source_settings_from_table(db_table))
    ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()

    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset=ds,
        formulas={
            "sum_lod": "SUM(SUM([sales] INCLUDE [category]))",
            "max_id": "MAX([id])",
        },
    )

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="city"),
            ds.find_field(title="sum_lod"),
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
    sum_lod = [int(row[1]) for row in data_rows]

    assert city == ["New York", "New Rochelle", None]
    assert sum_lod == [11, 1100, 110000]


def test_lod_only_in_filter(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Sales Sum": "SUM([Sales])",
            "Daily Profit Sum": "AVG(SUM([Sales] INCLUDE [Order Date]))",
        },
    )

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="City"),
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


def test_lod_in_filter_and_select(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Daily Profit Sum": "AVG(SUM([Sales] INCLUDE [Order Date]))",
        },
    )

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="City"),
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


def test_replace_original_dim_with_another(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Measure": "AVG(SUM([Sales] FIXED [City]))",
        },
    )

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="Category"),
            ds.find_field(title="Measure"),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK, result_resp.json
    data_rows = get_data_rows(result_resp)
    assert len(data_rows) == 3  # There are 3 categories


def test_bi_4534(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Sum": "SUM([Sales])",
            "Measure": "SUM([Sum]) - SUM(AGO([Sum], [Order Date]))",
        },
    )

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="Order Date"),
            ds.find_field(title="Measure"),
        ],
        fail_ok=True,
    )
    assert result_resp.status_code == HTTPStatus.OK


def test_bi_4652_measure_filter_with_total_in_select(api_v1, data_api_v2, dataset_id):
    data_api = data_api_v2
    ds = add_formulas_to_dataset(
        api_v1=api_v1,
        dataset_id=dataset_id,
        formulas={
            "Measure": "SUM([Sales])",
            "Total Measure": "SUM([Sales] FIXED)",
        },
    )

    result_resp = data_api.get_result(
        dataset=ds,
        fields=[
            ds.find_field(title="City"),
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
