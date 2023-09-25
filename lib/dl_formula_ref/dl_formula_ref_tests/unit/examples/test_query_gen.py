from __future__ import annotations

from typing import TYPE_CHECKING

from dl_connector_clickhouse.formula.constants import ClickHouseDialect
from dl_formula.core.datatype import DataType
from dl_formula.parser.factory import get_parser
from dl_formula.shortcuts import n
from dl_formula_ref.examples.config import (
    ExampleConfig,
    ExampleSource,
)
from dl_formula_ref.examples.data_table import DataColumn
from dl_formula_ref.examples.query import (
    FormulaContext,
    RawQueryContext,
)
from dl_formula_ref.examples.query_gen import QueryGenerator


if TYPE_CHECKING:
    from dl_formula.core import nodes


def parse(formula: str) -> nodes.Formula:
    return get_parser().parse(formula)


def test_gen_query_from_example():
    example = ExampleConfig(
        dialect=ClickHouseDialect.CLICKHOUSE_21_8,
        source=ExampleSource(
            columns=[("int_value", DataType.INTEGER), ("str_value", DataType.STRING)],
            data=[],
        ),
        formula_fields=[
            ("int_value", "[int_value]"),
            ("str_value", "[str_value]"),
            ("test_1", "[int_value] * 2"),
            ("test_2", 'CONCAT([str_value], "AAA")'),
            ("test_3", "INT([str_value])"),
        ],
        group_by=["[int_value]", "[str_value]"],
        order_by=["[int_value]"],
    )
    query_gen = QueryGenerator()
    actual_query = query_gen.generate_query(example=example)
    expected_query = RawQueryContext(
        result_columns=[
            DataColumn(name="int_value", data_type=DataType.INTEGER),
            DataColumn(name="str_value", data_type=DataType.STRING),
            DataColumn(name="test_1", data_type=DataType.INTEGER),
            DataColumn(name="test_2", data_type=DataType.STRING),
            DataColumn(name="test_3", data_type=DataType.INTEGER),
        ],
        select=[
            FormulaContext(formula=n.formula(n.field("int_value")), alias="int_value"),
            FormulaContext(formula=n.formula(n.field("str_value")), alias="str_value"),
            FormulaContext(
                formula=n.formula(n.binary("*", n.field("int_value"), n.lit(2))),
                alias="test_1",
            ),
            FormulaContext(
                formula=n.formula(n.func.CONCAT(n.field("str_value"), n.lit("AAA"))),
                alias="test_2",
            ),
            FormulaContext(
                formula=n.formula(n.func.INT(n.field("str_value"))),
                alias="test_3",
            ),
        ],
        group_by=[
            FormulaContext(formula=n.formula(n.field("int_value")), alias=None),
            FormulaContext(formula=n.formula(n.field("str_value")), alias=None),
        ],
        order_by=[
            FormulaContext(formula=n.formula(n.field("int_value")), alias=None),
        ],
    )
    assert actual_query == expected_query
