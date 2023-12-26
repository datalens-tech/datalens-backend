from collections import defaultdict
import functools
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
from dl_api_lib_testing.helpers.lookup_checkers import check_ago_data
from dl_constants.enums import (
    QueryProcessingMode,
    UserDataType,
    WhereClauseOperation,
)
from dl_core_testing.database import (
    C,
    make_table,
)
from dl_core_testing.testcases.service_base import DbServiceFixtureTextClass
from dl_testing.regulated_test import (
    Feature,
    RegulatedTestCase,
    for_features,
)


class DefaultBasicExtAggregationTestSuite(
    RegulatedTestCase, DataApiTestBase, DatasetTestBase, DbServiceFixtureTextClass
):
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

    def test_null_dimensions(self, request, control_api, data_api, db, saved_connection_id):
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
        request.addfinalizer(functools.partial(db.drop_table, db_table.table))

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
        request,
        control_api,
        data_api,
        saved_connection_id,
        db,
    ):
        db_table = make_table(db=db)
        request.addfinalizer(functools.partial(db.drop_table, db_table.table))
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
        request,
        control_api,
        data_api,
        saved_connection_id,
        db,
    ):
        db_table = make_table(db=db)
        request.addfinalizer(functools.partial(db.drop_table, db_table.table))

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


class DefaultBasicLookupFunctionTestSuite(
    RegulatedTestCase, DataApiTestBase, DatasetTestBase, DbServiceFixtureTextClass
):
    def test_ago_any_db(self, request, saved_connection_id, control_api, data_api, db):
        db_table = make_table(db=db)
        request.addfinalizer(functools.partial(db.drop_table, db_table.table))
        ds = create_basic_dataset(
            api_v1=control_api,
            connection_id=saved_connection_id,
            data_source_settings=data_source_settings_from_table(table=db_table),
            formulas={
                "sum": "SUM([int_value])",
                "ago": 'AGO([sum], [date_value], "day", 2)',
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="date_value"),
                ds.find_field(title="sum"),
                ds.find_field(title="ago"),
            ],
            order_by=[
                ds.find_field(title="date_value"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)

        check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=2, day_offset=2)

    def test_triple_ago_any_db(self, request, saved_connection_id, control_api, data_api, db):
        db_table = make_table(db)
        request.addfinalizer(functools.partial(db.drop_table, db_table.table))
        ds = create_basic_dataset(
            api_v1=control_api,
            connection_id=saved_connection_id,
            data_source_settings=data_source_settings_from_table(table=db_table),
            formulas={
                "sum": "SUM([int_value])",
                "ago_1": 'AGO([sum], [date_value], "day", 1)',
                "ago_2": 'AGO([sum], [date_value], "day", 2)',
                "ago_3": 'AGO([sum], [date_value], "day", 3)',
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="date_value"),
                ds.find_field(title="sum"),
                ds.find_field(title="ago_1"),
                ds.find_field(title="ago_2"),
                ds.find_field(title="ago_3"),
            ],
            order_by=[
                ds.find_field(title="date_value"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)

        check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=2, day_offset=1)
        check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=3, day_offset=2)
        check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=4, day_offset=3)

    def test_ago_any_db_multisource(self, request, saved_connection_id, control_api, data_api, db):
        connection_id = saved_connection_id
        table_1 = make_table(db)
        table_2 = make_table(db)

        def teardown(db, *tables):
            for table in tables:
                db.drop_table(table)

        request.addfinalizer(functools.partial(teardown, db, table_1.table, table_2.table))

        ds = Dataset()
        ds.sources["source_1"] = ds.source(
            connection_id=connection_id,
            **data_source_settings_from_table(table=table_1),
        )
        ds.sources["source_2"] = ds.source(
            connection_id=connection_id,
            **data_source_settings_from_table(table=table_2),
        )
        ds.source_avatars["avatar_1"] = ds.sources["source_1"].avatar()
        ds.source_avatars["avatar_2"] = ds.sources["source_2"].avatar()
        ds.avatar_relations["rel_1"] = (
            ds.source_avatars["avatar_1"]
            .join(ds.source_avatars["avatar_2"])
            .on(ds.col("string_value") == ds.col("string_value"))
        )

        ds.result_schema["date_1"] = ds.source_avatars["avatar_1"].field(source="date_value")
        ds.result_schema["int_2"] = ds.source_avatars["avatar_2"].field(source="int_value")
        ds.result_schema["sum"] = ds.field(formula="SUM([int_2])")
        ds.result_schema["ago"] = ds.field(formula='AGO([sum], [date_1], "day", 2)')
        ds_resp = control_api.apply_updates(dataset=ds, fail_ok=True)
        assert ds_resp.status_code == HTTPStatus.OK, ds_resp.response_errors
        ds = ds_resp.dataset
        ds = control_api.save_dataset(ds).dataset

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="date_1"),
                ds.find_field(title="sum"),
                ds.find_field(title="ago"),
            ],
            order_by=[
                ds.find_field(title="date_1"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)

        check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=2, day_offset=2)

    def test_nested_ago(self, request, saved_connection_id, control_api, data_api, db):
        db_table = make_table(db)
        request.addfinalizer(functools.partial(db.drop_table, db_table.table))
        ds = create_basic_dataset(
            api_v1=control_api,
            connection_id=saved_connection_id,
            data_source_settings=data_source_settings_from_table(table=db_table),
            formulas={
                "sum": "SUM([int_value])",
                "ago_1": 'AGO([sum], [date_value], "day", 1)',
                "ago_2": 'AGO([ago_1], [date_value], "day", 1)',
                "ago_3": 'AGO([ago_2], [date_value], "day", 1)',
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="date_value"),
                ds.find_field(title="sum"),
                ds.find_field(title="ago_1"),
                ds.find_field(title="ago_2"),
                ds.find_field(title="ago_3"),
            ],
            order_by=[
                ds.find_field(title="date_value"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)

        check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=2, day_offset=1)
        check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=3, day_offset=2)
        check_ago_data(data_rows=data_rows, date_idx=0, value_idx=1, ago_idx=4, day_offset=3)

    def test_month_ago_for_shorter_month(self, request, db, saved_connection_id, control_api, data_api):
        any_db_table_200 = make_table(db, rows=200)
        request.addfinalizer(functools.partial(db.drop_table, any_db_table_200.table))

        ds = create_basic_dataset(
            api_v1=control_api,
            connection_id=saved_connection_id,
            data_source_settings=data_source_settings_from_table(table=any_db_table_200),
            formulas={
                "new_date_value": "#2021-01-01# + [int_value]",
                "sum": "SUM([int_value])",
                "ago": 'AGO([sum], [new_date_value], "month", 1)',
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="new_date_value"),
                ds.find_field(title="sum"),
                ds.find_field(title="ago"),
            ],
            filters=[
                ds.find_field(title="new_date_value").filter(op=WhereClauseOperation.EQ, values=["2021-02-28"]),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json
        data_rows = get_data_rows(result_resp)

        # Check whether rows are duplicated
        assert len(data_rows) == 1


class DefaultBasicWindowFunctionTestSuite(
    RegulatedTestCase, DataApiTestBase, DatasetTestBase, DbServiceFixtureTextClass
):
    feature_window_functions = Feature("window_functions")

    @for_features(feature_window_functions)
    def test_window_functions(self, control_api, data_api, saved_dataset):
        ds = add_formulas_to_dataset(
            api_v1=control_api,
            dataset=saved_dataset,
            formulas={
                "Group Sales": "SUM([sales])",
                "Rank of Sales": 'RANK([Group Sales], "asc" TOTAL)',
                "Unique Rank of Sales": 'RANK_UNIQUE([Group Sales], "asc" TOTAL)',
                "Rank of City Sales for Date": 'RANK([Group Sales], "asc" AMONG [city])',
                "Total Sales": "SUM([Group Sales] TOTAL)",
                "Date Sales": "SUM([Group Sales] WITHIN [order_date])",
                "City Sales": "SUM([Group Sales] AMONG [order_date])",
                "Total RSUM": 'RSUM([Group Sales], "asc" TOTAL)',
            },
        )

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="order_date"),
                ds.find_field(title="city"),
                ds.find_field(title="Group Sales"),
                ds.find_field(title="Rank of Sales"),
                ds.find_field(title="Unique Rank of Sales"),
                ds.find_field(title="Rank of City Sales for Date"),
                ds.find_field(title="Total Sales"),
                ds.find_field(title="Date Sales"),
                ds.find_field(title="City Sales"),
                ds.find_field(title="Total RSUM"),
            ],
            order_by=[
                ds.find_field(title="order_date"),
                ds.find_field(title="city"),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == HTTPStatus.OK, result_resp.json

        data_rows = get_data_rows(result_resp)
        cnt = len(data_rows)

        # TODO: More thorough tests

        # [Rank of Sales] values are a subset of the full range of row numbers
        assert {row[3] for row in data_rows}.issubset({str(i) for i in range(1, cnt + 1)})

        # There are as many [Unique Rank of Sales] values as there are rows
        assert {row[4] for row in data_rows} == ({str(i) for i in range(1, cnt + 1)})

        # [Rank of City Sales for Date] values are not greater than the number of [City] values
        assert len({row[5] for row in data_rows}) <= len({row[1] for row in data_rows})

        # all rows have the same [Total Sales] value
        assert len({row[6] for row in data_rows}) == 1

        # as many values of [Date Sales] as there are [order_date] values (or less - because there may be duplicates)
        assert len({row[7] for row in data_rows}) <= len({row[0] for row in data_rows})

        # as many values of [City Sales] as there are [City] values (or less - because there may be duplicates)
        assert len({row[8] for row in data_rows}) <= len({row[1] for row in data_rows})

        for i in range(1, len(data_rows)):
            # RSUM = previous RSUM value + value of current arg
            assert pytest.approx(float(data_rows[i][9])) == float(data_rows[i - 1][9]) + float(data_rows[i][2])


class DefaultBasicComplexQueryTestSuite(
    DefaultBasicExtAggregationTestSuite,
    DefaultBasicLookupFunctionTestSuite,
    DefaultBasicWindowFunctionTestSuite,
):
    """Put them all together"""

    query_processing_mode = QueryProcessingMode.native_wf
