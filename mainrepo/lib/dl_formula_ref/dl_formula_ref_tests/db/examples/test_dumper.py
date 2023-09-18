import sqlalchemy as sa

from dl_formula.core.datatype import DataType
from dl_formula_ref.examples.data_table import (
    DataColumn,
    DataTable,
)
from dl_formula_ref.examples.dumper import get_dumper
from dl_formula_ref.examples.query import CompiledQueryContext


def test_select_original_data(dbe):
    db = dbe.db
    original_data_table = DataTable(
        columns=[
            DataColumn(name="int_value", data_type=DataType.INTEGER),
            DataColumn(name="str_value", data_type=DataType.STRING),
        ],
        rows=[
            [11, "qwe"],
            [45, "rty"],
        ],
    )
    dumper = get_dumper(db=db)
    with dumper.temporary_data_table(data_table=original_data_table) as table_ref:
        sa_query = (
            sa.select(
                [
                    sa.literal_column("int_value"),
                    sa.literal_column("str_value"),
                ]
            )
            .select_from(sa.table(name=table_ref.name))
            .order_by(sa.literal_column("int_value"))
        )
        query_ctx = CompiledQueryContext(
            sa_query=sa_query,
            result_columns=original_data_table.columns,
        )
        loaded_data_table = dumper.execute_query(query_ctx=query_ctx)

    assert loaded_data_table == original_data_table
