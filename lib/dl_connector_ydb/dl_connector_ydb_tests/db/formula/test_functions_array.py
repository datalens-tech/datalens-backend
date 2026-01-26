import contextlib
from typing import (
    Generator,
    Optional,
)

import pytest
import sqlalchemy as sa

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.table import generate_sample_data
from dl_formula_testing.testcases.functions_array import DefaultArrayFunctionFormulaConnectorTestSuite

from dl_connector_ydb_tests.db.formula.base import YQLTestBase


class ArrayFunctionYDBTestSuite(DefaultArrayFunctionFormulaConnectorTestSuite):
    make_decimal_cast = None
    make_float_cast = "Float"
    make_float_array_cast = "List<Float?>"
    make_str_array_cast = "List<String?>"

    # Using VIEW instead of TABLE
    # supports_array_columns = False

    def test_startswith_string_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval("STARTSWITH([arr_str_value], [arr_str_value])", from_=data_table)
        assert not dbe.eval('STARTSWITH([arr_str_value], ARRAY("", "cde", NULL))', from_=data_table)

    def test_array_contains_all_string_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('CONTAINS_ALL([arr_str_value], ARRAY("cde"))', from_=data_table)
        assert dbe.eval('CONTAINS_ALL([arr_str_value], ARRAY("cde", NULL))', from_=data_table)
        assert not dbe.eval('CONTAINS_ALL(ARRAY("cde"), [arr_str_value])', from_=data_table)

    def test_array_contains_any_string_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        assert dbe.eval('CONTAINS_ANY([arr_str_value], ARRAY("cde"))', from_=data_table)
        assert dbe.eval('CONTAINS_ANY([arr_str_value], ARRAY("123", NULL))', from_=data_table)
        assert dbe.eval('CONTAINS_ANY(ARRAY("cde"), [arr_str_value])', from_=data_table)


class YQLViewTableTestBase(YQLTestBase):
    @contextlib.contextmanager
    def make_data_table(self, dbe: DbEvaluator, table_schema_name: Optional[str]) -> Generator[sa.Table, None, None]:
        sample_data = generate_sample_data(add_arrays=True)

        table_spec = self.generate_table_spec(table_name_prefix="test_view")
        table = self.make_sa_table(db=dbe.db, table_spec=table_spec, table_schema_name=table_schema_name)

        query = ""
        query += f"create view `{table_spec.table_name}` with (security_invoker = TRUE) as select * from (\n"

        # Convert sample_data into view select statement
        for index, row in enumerate(sample_data):
            query += " select\n"
            query += f"cast({ row['id'] } as Int64) as id,\n"
            query += f"cast({ row['int_value'] } as Int64) as int_value,\n"
            query += f"cast({ row['date_value'].strftime('%Y-%m-%d') } as Date) as date_value,\n"
            query += f"DateTime::FromMicroseconds({ int(row['datetime_value'].timestamp() * 1_000_000) }) as datetime_value,\n"
            query += f"'{ row['str_value'] }' as str_value,\n"
            query += "Nothing(String?) as str_null_value,\n"

            arr_int_value_str = [str(v) if v is not None else "NULL" for v in row["arr_int_value"]]
            query += f"AsList({ (',').join(arr_int_value_str) }) as arr_int_value,\n"

            arr_float_value_str = [str(v) if v is not None else "NULL" for v in row["arr_float_value"]]
            query += f"AsList({ (',').join(arr_float_value_str) }) as arr_float_value,\n"

            qt = '"'
            att_str_value_str = [(qt + v + qt) if v is not None else "NULL" for v in row["arr_str_value"]]
            query += f"AsList({ (',').join(att_str_value_str) }) as arr_str_value,\n"
            query += "Nothing(List<Int64?>?) as arr_int_null_value,\n"
            query += "Nothing(List<Float?>?) as arr_float_null_value,\n"
            query += "Nothing(List<String?>?) as arr_str_null_value\n"

            if index != len(sample_data) - 1:
                query += " union all\n"

        query += ")"

        dbe.db.get_current_connection().connection.cursor().execute_scheme(query)

        try:
            yield table
        finally:
            dbe.db.get_current_connection().connection.cursor().execute_scheme(f"DROP VIEW `{table_spec.table_name}`;")


class TestArrayFunctionYDB(YQLViewTableTestBase, ArrayFunctionYDBTestSuite):
    @pytest.mark.xfail(reason="UNNEST Not Implemented")
    def test_unnest_array(self, dbe: DbEvaluator, data_table: sa.Table) -> None:
        super().test_unnest_array(dbe, data_table)
