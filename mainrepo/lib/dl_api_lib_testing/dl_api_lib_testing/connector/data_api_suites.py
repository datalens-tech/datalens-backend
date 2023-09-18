import datetime
import json
from typing import (
    Any,
    ClassVar,
)

import pytest

from dl_api_client.dsmaker.api.data_api import SyncHttpDataApiV2
from dl_api_client.dsmaker.api.dataset_api import SyncHttpDatasetApiV1
from dl_api_client.dsmaker.primitives import Dataset
from dl_api_client.dsmaker.shortcuts.result_data import get_data_rows
from dl_api_lib_testing.data_api_base import (
    DataApiTestParams,
    StandardizedDataApiTestBase,
)
from dl_constants.enums import (
    BIType,
    WhereClauseOperation,
)
from dl_core_testing.database import (
    C,
    Db,
    make_table,
)


class DefaultConnectorDataResultTestSuite(StandardizedDataApiTestBase):
    do_test_arrays: ClassVar[bool] = False

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

    def test_array_contains_filter(
        self,
        db: Db,
        saved_connection_id: str,
        dataset_params: dict,
        dataset_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        if not self.do_test_arrays:
            pytest.skip("do_test_arrays = False")

        columns = [
            C("int_value", BIType.integer, vg=lambda rn, **kwargs: rn),
            C("array_int_value", BIType.array_int, vg=lambda rn, **kwargs: [i for i in reversed(range(rn))]),
            C(
                "array_str_value",
                BIType.array_str,
                vg=lambda rn, **kwargs: [str(i) if i != 5 else None for i in reversed(range(rn))],
            ),
            C(
                "array_float_value",
                BIType.array_float,
                vg=lambda rn, **kwargs: [i / 100.0 for i in reversed(range(rn))],
            ),
        ]
        db_table = make_table(db, columns=columns)
        params = self.get_dataset_params(dataset_params, db_table)
        ds = self.make_basic_dataset(dataset_api, connection_id=saved_connection_id, dataset_params=params)

        def check_filter(field_title: str, filter_value: Any) -> None:
            result_resp = data_api.get_result(
                dataset=ds,
                fields=[
                    ds.find_field(title="int_value"),
                    ds.find_field(title=field_title),
                ],
                filters=[
                    ds.find_field(title=field_title).filter(
                        op=WhereClauseOperation.CONTAINS,
                        values=[filter_value],
                    ),
                ],
                fail_ok=True,
            )
            assert result_resp.status_code == 200, result_resp.json
            data_rows = get_data_rows(result_resp)
            assert data_rows, repr(filter_value)
            for row in data_rows:
                result_array = json.loads(row[1])
                assert filter_value in result_array

        check_filter("array_int_value", 3)
        check_filter("array_str_value", "3")
        check_filter("array_float_value", 0.03)
        check_filter("array_str_value", None)

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
    def test_array_contains_field(
        self,
        db: Db,
        saved_connection_id: str,
        dataset_params: dict,
        dataset_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
        field_title: str,
        filter_field_title: str,
        is_numeric: bool,
    ) -> None:
        if not self.do_test_arrays:
            pytest.skip("do_test_arrays = False")

        columns = [
            C("int_value", BIType.integer, vg=lambda rn, **kwargs: 3),
            C("str_value", BIType.string, vg=lambda rn, **kwargs: "3"),
            C("float_value", BIType.float, vg=lambda rn, **kwargs: 0.03),
            C("none_value", BIType.float, vg=lambda rn, **kwargs: None),
            C("array_int_value", BIType.array_int, vg=lambda rn, **kwargs: [i for i in reversed(range(rn))]),
            C("array_str_value", BIType.array_str, vg=lambda rn, **kwargs: [str(i) for i in reversed(range(rn))]),
            C(
                "array_float_value",
                BIType.array_float,
                vg=lambda rn, **kwargs: [i / 100.0 if i != 5 else None for i in reversed(range(rn))],
            ),
        ]
        db_table = make_table(db, columns=columns)
        params = self.get_dataset_params(dataset_params, db_table)
        ds = self.make_basic_dataset(dataset_api, connection_id=saved_connection_id, dataset_params=params)

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
        ds.result_schema[new_field_name].cast = BIType.float
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


class DefaultConnectorDataGroupByFormulaTestSuite(StandardizedDataApiTestBase):
    def test_complex_result(
        self,
        saved_dataset: Dataset,
        data_api_test_params: DataApiTestParams,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        ds = saved_dataset
        ds.result_schema["CityNameLength"] = ds.field(formula=f"LEN([{data_api_test_params.distinct_field}])")

        grouped_resp = self.get_result_ordered(
            ds, data_api, field_names=(data_api_test_params.two_dims[0], "CityNameLength"), order_by=("CityNameLength",)
        )
        grouped_rows = get_data_rows(grouped_resp)

        min_row_cnt = 10  # just an arbitrary number
        assert len(grouped_rows) > min_row_cnt


class DefaultConnectorDataRangeTestSuite(StandardizedDataApiTestBase):
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


class DefaultConnectorDataDistinctTestSuite(StandardizedDataApiTestBase):
    def test_basic_distinct(
        self,
        saved_dataset: Dataset,
        data_api_test_params: DataApiTestParams,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        ds = saved_dataset

        distinct_resp = self.get_distinct(ds, data_api, field_name=data_api_test_params.distinct_field)
        distinct_rows = get_data_rows(distinct_resp)
        min_distinct_row_cnt = 10  # just an arbitrary number
        assert len(distinct_rows) > min_distinct_row_cnt
        values = [row[0] for row in distinct_rows]
        assert len(set(values)) == len(values), "Values are not unique"

    def test_date_filter_distinct(
        self,
        db: Db,
        saved_connection_id: str,
        dataset_params: dict,
        dataset_api: SyncHttpDatasetApiV1,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        columns = [
            C(name="date_val", user_type=BIType.date, nullable=True),
        ]
        data = [
            {"date_val": datetime.date(2002, 1, 2)},
            {"date_val": datetime.date(2023, 4, 2)},
        ]
        db_table = make_table(db, columns=columns, data=data)
        params = self.get_dataset_params(dataset_params, db_table)
        ds = self.make_basic_dataset(dataset_api, connection_id=saved_connection_id, dataset_params=params)

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


class DefaultConnectorDataPreviewTestSuite(StandardizedDataApiTestBase):
    def test_basic_distinct(
        self,
        saved_dataset: Dataset,
        data_api: SyncHttpDataApiV2,
    ) -> None:
        ds = saved_dataset

        preview_resp = self.get_preview(ds, data_api)
        preview_rows = get_data_rows(preview_resp)
        min_preview_row_cnt = 10  # just an arbitrary number
        assert len(preview_rows) > min_preview_row_cnt
