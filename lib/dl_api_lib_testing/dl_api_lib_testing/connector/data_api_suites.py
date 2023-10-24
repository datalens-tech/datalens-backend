import datetime
import functools
import json
from typing import Any

import pytest

from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import (
    Dataset,
    WhereClause,
)
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_lib_testing.data_api_base import (
    DataApiTestParams,
    StandardizedDataApiTestBase,
)
from dl_constants.enums import (
    UserDataType,
    WhereClauseOperation,
)
from dl_core_testing.database import (
    C,
    Db,
    make_table,
)
from dl_testing.regulated_test import (
    Feature,
    RegulatedTestCase,
    for_features,
)


class DefaultConnectorDataResultTestSuite(StandardizedDataApiTestBase, RegulatedTestCase):
    array_support = Feature("Connector supports arrays")

    def test_basic_result(
        self,
        saved_dataset: Dataset,
        data_api_test_params: DataApiTestParams,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        ds = saved_dataset
        ds.result_schema["Measure"] = ds.field(formula=f"SUM([{data_api_test_params.summable_field}])")

        result_resp_1 = self.get_result(ds, data_api, field_names=("Measure", data_api_test_params.two_dims[0]))
        result_resp_2 = self.get_result(ds, data_api, field_names=("Measure", *data_api_test_params.two_dims))

        rows_1 = get_data_rows(result_resp_1)
        rows_2 = get_data_rows(result_resp_2)

        min_row_cnt = 2  # just an arbitrary number
        assert len(rows_1) > min_row_cnt
        assert len(rows_2) > len(rows_1)

        total_sum_1 = sum(float(row[0]) for row in rows_1)
        total_sum_2 = sum(float(row[0]) for row in rows_2)

        assert pytest.approx(total_sum_1) == total_sum_2

    def test_duplicated_expressions(
        self,
        saved_dataset: Dataset,
        data_api_test_params: DataApiTestParams,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        ds = saved_dataset
        # yes, they are identical
        ds.result_schema["Measure 1"] = ds.field(formula=f"SUM([{data_api_test_params.summable_field}])")
        ds.result_schema["Measure 2"] = ds.field(formula=f"SUM([{data_api_test_params.summable_field}])")

        result_resp = self.get_result(
            ds, data_api, field_names=("Measure 1", "Measure 2", data_api_test_params.two_dims[0])
        )
        rows = get_data_rows(result_resp)
        min_row_cnt = 2  # just an arbitrary number
        assert len(rows) > min_row_cnt
        assert all(row[0] == row[1] for row in rows)

    def _test_contains(
        self,
        request: pytest.FixtureRequest,
        db: Db,
        saved_connection_id: str,
        dataset_params: dict,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        filter_op: WhereClauseOperation,
    ) -> None:
        columns = [
            C("int_value", UserDataType.integer, vg=lambda rn, **kwargs: rn),
            C("array_int_value", UserDataType.array_int, vg=lambda rn, **kwargs: [i for i in reversed(range(rn))]),
            C(
                "array_str_value",
                UserDataType.array_str,
                vg=lambda rn, **kwargs: [str(i) if i != 5 else None for i in reversed(range(rn))],
            ),
            C(
                "array_float_value",
                UserDataType.array_float,
                vg=lambda rn, **kwargs: [i / 100.0 for i in reversed(range(rn))],
            ),
        ]
        db_table = make_table(db, columns=columns)
        request.addfinalizer(functools.partial(db.drop_table, db_table.table))

        params = self.get_dataset_params(dataset_params, db_table)
        ds = self.make_basic_dataset(control_api, connection_id=saved_connection_id, dataset_params=params)

        def check_filter(field_title: str, filter_value: Any) -> None:
            result_resp = data_api.get_result(
                dataset=ds,
                fields=[
                    ds.find_field(title="int_value"),
                    ds.find_field(title=field_title),
                ],
                filters=[
                    ds.find_field(title=field_title).filter(
                        op=filter_op,
                        values=[filter_value],
                    ),
                ],
            )

            data_rows = get_data_rows(result_resp)
            assert data_rows, repr(filter_value)
            for row in data_rows:
                result_array = json.loads(row[1])
                if filter_op == WhereClauseOperation.CONTAINS:
                    assert filter_value in result_array
                elif filter_op == WhereClauseOperation.NOTCONTAINS:
                    assert filter_value not in result_array
                else:
                    raise ValueError(f"Unknown operation {filter_op}")

        check_filter("array_int_value", 3)
        check_filter("array_str_value", "3")
        check_filter("array_float_value", 0.03)
        check_filter("array_str_value", None)

    @for_features(array_support)
    def test_array_contains_filter(
        self,
        request: pytest.FixtureRequest,
        db: Db,
        saved_connection_id: str,
        dataset_params: dict,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        self._test_contains(
            request, db, saved_connection_id, dataset_params, control_api, data_api, WhereClauseOperation.CONTAINS
        )

    @for_features(array_support)
    def test_array_not_contains_filter(
        self,
        request: pytest.FixtureRequest,
        db: Db,
        saved_connection_id: str,
        dataset_params: dict,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        self._test_contains(
            request, db, saved_connection_id, dataset_params, control_api, data_api, WhereClauseOperation.NOTCONTAINS
        )

    @pytest.mark.parametrize(
        "field_title,filter_field_title,is_numeric",
        (
            ("array_int_value", "int_value", True),
            ("array_str_value", "str_value", False),
            ("array_str_value", "concat_const", False),
            ("array_str_value", "concat_field", False),
            ("array_float_value", "float_value", True),
            ("array_float_value", "none_value", False),
        ),
    )
    @for_features(array_support)
    def test_array_contains_field(
        self,
        request: pytest.FixtureRequest,
        db: Db,
        saved_connection_id: str,
        dataset_params: dict,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        field_title: str,
        filter_field_title: str,
        is_numeric: bool,
    ) -> None:
        columns = [
            C("int_value", UserDataType.integer, vg=lambda rn, **kwargs: 3),
            C("str_value", UserDataType.string, vg=lambda rn, **kwargs: "3"),
            C("float_value", UserDataType.float, vg=lambda rn, **kwargs: 0.03),
            C("none_value", UserDataType.float, vg=lambda rn, **kwargs: None),
            C("array_int_value", UserDataType.array_int, vg=lambda rn, **kwargs: [i for i in reversed(range(rn))]),
            C("array_str_value", UserDataType.array_str, vg=lambda rn, **kwargs: [str(i) for i in reversed(range(rn))]),
            C(
                "array_float_value",
                UserDataType.array_float,
                vg=lambda rn, **kwargs: [i / 100.0 if i != 5 else None for i in reversed(range(rn))],
            ),
        ]
        db_table = make_table(db, columns=columns)
        request.addfinalizer(functools.partial(db.drop_table, db_table.table))
        params = self.get_dataset_params(dataset_params, db_table)
        ds = self.make_basic_dataset(control_api, connection_id=saved_connection_id, dataset_params=params)

        filter_name = filter_field_title + "_ff"
        ds.result_schema["concat_const"] = ds.field(formula="CONCAT('3', '')")
        ds.result_schema["concat_field"] = ds.field(formula="CONCAT([str_value], '')")
        ds.result_schema[filter_name] = ds.field(formula=f"CONTAINS([{field_title}], [{filter_field_title}])")

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title=filter_field_title),
                ds.find_field(title=field_title),
            ],
            filters=[
                ds.find_field(title=filter_name).filter(
                    op=WhereClauseOperation.EQ,
                    values=[True],
                ),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == 200, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert data_rows, filter_field_title
        for row in data_rows:
            result_value = json.loads(row[0]) if is_numeric else row[0]
            result_array = json.loads(row[1])
            assert result_value in result_array

    def test_dates(
        self, saved_dataset: Dataset, data_api_test_params: DataApiTestParams, data_api: SyncHttpDataApiV2
    ) -> None:
        ds = saved_dataset
        new_field_name = "New shiny field"
        ds.result_schema[new_field_name] = ds.field(
            formula=f"IF [{data_api_test_params.date_field}] > DATE('2020-01-01') THEN 1 ELSE 2 END"
        )
        ds.result_schema[new_field_name].cast = UserDataType.float
        result_resp = self.get_result(ds, data_api, field_names=(data_api_test_params.date_field, new_field_name))
        assert result_resp.status_code == 200, result_resp.json

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title=data_api_test_params.date_field)],
            filters=[
                ds.find_field(title=data_api_test_params.date_field).filter(
                    op=WhereClauseOperation.GT,
                    values=["1990-01-01"],
                )
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == 200, result_resp.json
        assert get_data_rows(result_resp)

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[ds.find_field(title=data_api_test_params.date_field)],
            filters=[
                ds.find_field(title=data_api_test_params.date_field).filter(
                    op=WhereClauseOperation.BETWEEN,
                    values=["1990-01-01", "2023-10-02"],
                )
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == 200, result_resp.json
        assert get_data_rows(result_resp)

    def test_get_result_with_formula_in_where(
        self, saved_dataset: Dataset, data_api_test_params: DataApiTestParams, data_api: SyncHttpDataApiV2
    ) -> None:
        ds = saved_dataset
        ds.result_schema["Summable Percentage"] = ds.field(formula=f"[{data_api_test_params.summable_field}] * 100")

        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title="Summable Percentage"),
            ],
            filters=[
                ds.find_field(title="Summable Percentage").filter(WhereClauseOperation.GT, [30]),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == 200, result_resp.json
        data_rows = get_data_rows(result_resp)
        assert data_rows
        assert all(float(row[0]) > 30 for row in data_rows)

    def test_get_result_with_string_filter_operations_for_numbers(
        self, saved_dataset: Dataset, data_api_test_params: DataApiTestParams, data_api: SyncHttpDataApiV2
    ) -> None:
        ds = saved_dataset
        result_resp = data_api.get_result(
            dataset=ds,
            fields=[
                ds.find_field(title=data_api_test_params.summable_field),
            ],
            filters=[
                ds.find_field(title=data_api_test_params.summable_field).filter(WhereClauseOperation.ICONTAINS, ["2"]),
            ],
            fail_ok=True,
        )
        assert result_resp.status_code == 200, result_resp.json

        data_rows = get_data_rows(result_resp)
        values: set[str] = {row[0] for row in data_rows}
        assert len(values) > 1  # we just need to make sure there are several different values
        assert all("2" in value for value in values), values


class DefaultConnectorDataGroupByFormulaTestSuite(StandardizedDataApiTestBase, RegulatedTestCase):
    def test_ordered_result(
        self,
        saved_dataset: Dataset,
        data_api_test_params: DataApiTestParams,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        ds = saved_dataset
        grouped_resp = self.get_result_ordered(
            ds,
            data_api,
            field_names=(data_api_test_params.two_dims[0], data_api_test_params.distinct_field),
            order_by=(data_api_test_params.distinct_field,),
        )
        grouped_rows = get_data_rows(grouped_resp)

        min_row_cnt = 5  # just an arbitrary number
        assert len(grouped_rows) > min_row_cnt

    def test_complex_result(
        self,
        saved_dataset: Dataset,
        data_api_test_params: DataApiTestParams,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        ds = saved_dataset
        ds.result_schema["LengthField"] = ds.field(formula=f"LEN([{data_api_test_params.distinct_field}])")

        grouped_resp = self.get_result_ordered(
            ds, data_api, field_names=(data_api_test_params.two_dims[0], "LengthField"), order_by=("LengthField",)
        )
        grouped_rows = get_data_rows(grouped_resp)

        min_row_cnt = 5  # just an arbitrary number
        assert len(grouped_rows) > min_row_cnt


class DefaultConnectorDataRangeTestSuite(StandardizedDataApiTestBase, RegulatedTestCase):
    def test_basic_range(
        self,
        saved_dataset: Dataset,
        data_api_test_params: DataApiTestParams,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        ds = saved_dataset

        range_resp = self.get_range(ds, data_api, field_name=data_api_test_params.range_field)
        range_rows = get_data_rows(range_resp)
        min_val, max_val = float(range_rows[0][0]), float(range_rows[0][1])
        assert max_val > min_val


class DefaultConnectorDataDistinctTestSuite(StandardizedDataApiTestBase, RegulatedTestCase):
    def test_basic_distinct(
        self,
        saved_dataset: Dataset,
        data_api_test_params: DataApiTestParams,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        ds = saved_dataset

        distinct_resp = self.get_distinct(ds, data_api, field_name=data_api_test_params.distinct_field)
        distinct_rows = get_data_rows(distinct_resp)
        min_distinct_row_cnt = 5  # just an arbitrary number
        assert len(distinct_rows) > min_distinct_row_cnt
        values = [row[0] for row in distinct_rows]
        assert len(set(values)) == len(values), "Values are not unique"

    def test_distinct_with_nonexistent_filter(
        self,
        saved_dataset: Dataset,
        data_api_test_params: DataApiTestParams,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        ds = saved_dataset

        distinct_resp = data_api.get_distinct(
            dataset=ds,
            field=ds.find_field(title=data_api_test_params.distinct_field),
            filters=[WhereClause(column="idontexist", operation=WhereClauseOperation.EQ, values=[0])],
            ignore_nonexistent_filters=True,
        )
        assert distinct_resp.status_code == 200, distinct_resp.json

        distinct_rows = get_data_rows(distinct_resp)
        min_distinct_row_cnt = 5  # just an arbitrary number
        assert len(distinct_rows) > min_distinct_row_cnt
        values = [row[0] for row in distinct_rows]
        assert len(set(values)) == len(values), "Values are not unique"

    def test_date_filter_distinct(
        self,
        request: pytest.FixtureRequest,
        db: Db,
        saved_connection_id: str,
        dataset_params: dict,
        control_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        columns = [
            C(name="date_val", user_type=UserDataType.date, nullable=True),
        ]
        data = [
            {"date_val": datetime.date(2002, 1, 2)},
            {"date_val": datetime.date(2023, 4, 2)},
        ]
        db_table = make_table(db, columns=columns, data=data)
        request.addfinalizer(functools.partial(db.drop_table, db_table.table))

        params = self.get_dataset_params(dataset_params, db_table)
        ds = self.make_basic_dataset(control_api, connection_id=saved_connection_id, dataset_params=params)

        distinct_resp = data_api.get_distinct(
            dataset=ds,
            field=ds.find_field(title="date_val"),
            filters=[
                ds.find_field(title="date_val").filter(
                    op=WhereClauseOperation.ICONTAINS,
                    values=["2023-04-02"],
                ),
            ],
        )
        assert distinct_resp.status_code == 200, distinct_resp.json
        data_rows = get_data_rows(distinct_resp)
        assert len(data_rows) == 1

        distinct_resp = data_api.get_distinct(
            dataset=ds,
            field=ds.find_field(title="date_val"),
            filters=[
                ds.find_field(title="date_val").filter(
                    op=WhereClauseOperation.ICONTAINS,
                    values=["202"],
                ),
            ],
        )
        assert distinct_resp.status_code == 200, distinct_resp.json
        data_rows = get_data_rows(distinct_resp)
        assert len(data_rows) == 1


class DefaultConnectorDataPreviewTestSuite(StandardizedDataApiTestBase, RegulatedTestCase):
    def test_basic_preview(
        self,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        ds = saved_dataset

        preview_resp = self.get_preview(ds, data_api)
        preview_rows = get_data_rows(preview_resp)
        min_preview_row_cnt = 10  # just an arbitrary number
        assert len(preview_rows) > min_preview_row_cnt
