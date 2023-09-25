import sqlalchemy as sa

from dl_connector_clickhouse_tests.db.formula.base import ClickHouse_21_8TestBase
from dl_formula.core import nodes
from dl_formula.core.datatype import DataType
from dl_formula.translation import ext_nodes
from dl_formula.translation.context import TranslationCtx
from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_string import DefaultStringFunctionFormulaConnectorTestSuite


class StringFunctionClickHouseTestSuite(DefaultStringFunctionFormulaConnectorTestSuite):
    def test_utf8(self, dbe: DbEvaluator) -> None:
        # UTF8() is only available for CLICKHOUSE
        binary_str = TranslationCtx(expression=sa.text(r"'\xcf\xf0\xe8\xe2\xe5\xf2'"), data_type=DataType.STRING)
        assert (
            dbe.eval(
                nodes.Formula.make(
                    nodes.FuncCall.make(
                        name="UTF8",
                        args=[ext_nodes.CompiledExpression.make(binary_str), nodes.LiteralString.make("CP-1251")],
                    )
                )
            )
            == "Привет"
        )


class TestStringFunctionClickHouse_21_8(ClickHouse_21_8TestBase, StringFunctionClickHouseTestSuite):
    pass
