from collections import defaultdict
from http import HTTPStatus
from typing import Iterable

import pytest

from dl_api_client.dsmaker.primitives import (
    Dataset,
    ResultField,
)
from dl_api_client.dsmaker.shortcuts.dataset import (
    add_formulas_to_dataset,
    create_basic_dataset,
)
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_lib_testing.data_api_base import DataApiTestBase
from dl_api_lib_testing.dataset_base import DatasetTestBase
from dl_api_lib_testing.helpers.data_source import data_source_settings_from_table
from dl_constants.enums import UserDataType
from dl_core_testing.database import (
    C,
    make_table,
)
from dl_core_testing.testcases.service_base import DbServiceFixtureTextClass


class DefaultBasicExtAggregationTestSuite(DataApiTestBase, DatasetTestBase, DbServiceFixtureTextClass):
    def test_lod_fixed_single_dim_in_two_dim_query(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "sales sum": "SUM([sales])",
                "sales sum fx city": "SUM([sales] FIXED [city])",
                "sales sum fx category": "SUM([sales] FIXED [category])",
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="city"),
                ds.find_field(title="category"),
                ds.find_field(title="sales sum"),
                ds.find_field(title="sales sum fx city"),
                ds.find_field(title="sales sum fx category"),
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
            assert float(row[3]) == pytest.approx(sum_by_city[row[0]])
            assert float(row[4]) == pytest.approx(sum_by_category[row[1]])

    def test_null_dimensions(self, control_api, data_api, db, saved_connection_id):
        connection_id = saved_connection_id

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
            api_v1=control_api,
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

    def test_total_lod(self, control_api, data_api, saved_dataset):
        data_api = data_api
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "sales sum": "SUM([sales])",
                "sales sum total": "SUM([sales] FIXED)",
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
                ds.find_field(title="city"),
                ds.find_field(title="sales sum"),
                ds.find_field(title="sales sum total"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)

        total_sum = sum(float(row[1]) for row in data_rows)

        for row_idx, row in enumerate(data_rows):
            assert (
                float(row[2]) == pytest.approx(total_sum) == pytest.approx(expected_total_value)
            ), f"total sum doesn't match expected number in row {row_idx}"

    def test_total_lod_2(
        self,
        control_api,
        data_api,
        saved_connection_id,
        db,
    ):
        db_table = make_table(db=db)
        ds = create_basic_dataset(
            api_v1=control_api,
            connection_id=saved_connection_id,
            data_source_settings=data_source_settings_from_table(table=db_table),
            formulas={
                "div 2": "DIV([int_value], 2)",
                "div 3": "DIV([int_value], 3)",
                "Agg 1": "SUM([int_value])",
                "Agg 2": "SUM(SUM([int_value] INCLUDE [div 2]))",
                "Agg 3": "SUM(SUM(SUM([int_value] INCLUDE [div 2]) INCLUDE [div 3]))",
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

        def check_equality_of_totals(*field_names: str) -> None:
            simultaneous_values = get_single_row_data(field_names)
            assert len(set(simultaneous_values)) == 1
            assert next(iter(simultaneous_values)) == value_1

        check_equality_of_totals("Agg 1", "Agg 2")
        check_equality_of_totals("Agg 2", "Agg 3")
        check_equality_of_totals("Agg 1", "Agg 3")
        check_equality_of_totals("Agg 1", "Agg 2", "Agg 3")

    def test_lod_in_order_by(
        self,
        control_api,
        data_api,
        saved_connection_id,
        db,
    ):
        db_table = make_table(db=db)

        data_api = data_api
        ds = create_basic_dataset(
            api_v1=control_api,
            connection_id=saved_connection_id,
            data_source_settings=data_source_settings_from_table(table=db_table),
            formulas={
                "Dimension": "[int_value] % 3",
                "LOD Measure": "SUM([int_value]) / SUM([int_value] FIXED)",
            },
        )

        def get_data(order_by: list[ResultField]) -> list:
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


class DefaultBasicComplexQueryTestSuite(DefaultBasicExtAggregationTestSuite):
    """Put them all together"""
