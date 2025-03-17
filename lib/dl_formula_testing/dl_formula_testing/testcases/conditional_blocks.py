from typing import Optional

from dl_formula.core.datatype import DataType
from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.base import FormulaConnectorTestBase
from dl_formula_testing.util import to_str


class DefaultConditionalBlockFormulaConnectorTestSuite(FormulaConnectorTestBase):
    def test_if_block(self, dbe: DbEvaluator) -> None:
        # simple
        assert to_str(dbe.eval('IF 2 = 2 THEN "1st" ELSE "2nd" END')) == "1st"
        assert to_str(dbe.eval('IF 2 = 1 THEN "1st" ELSE "2nd" END')) == "2nd"

        # with ELSIF
        assert to_str(dbe.eval('IF 1 = 1 THEN "1st" ELSEIF 1 = 2 THEN "2nd" ELSE "3rd" END')) == "1st"
        assert to_str(dbe.eval('IF 2 = 1 THEN "1st" ELSEIF 2 = 2 THEN "2nd" ELSE "3rd" END')) == "2nd"
        assert to_str(dbe.eval('IF 3 = 1 THEN "1st" ELSEIF 3 = 2 THEN "2nd" ELSE "3rd" END')) == "3rd"

        # without ELSE
        assert to_str(dbe.eval('IF 2 = 2 THEN "1st" END')) == "1st"
        assert dbe.eval('IF 2 = 1 THEN "1st" END') is None
        assert to_str(dbe.eval('IF 1 = 1 THEN "1st" ELSEIF 2 = 2 THEN "2nd" END')) == "1st"
        assert to_str(dbe.eval('IF 1 = 2 THEN "1st" ELSEIF 2 = 2 THEN "2nd" END')) == "2nd"
        assert dbe.eval('IF 1 = 2 THEN "1st" ELSEIF 2 = 1 THEN "2nd" END') is None

    def test_case_block_basic(self, dbe: DbEvaluator) -> None:
        # "normal"
        assert to_str(dbe.eval('CASE 1 WHEN 1 THEN "1st" WHEN 2 THEN "2nd" ELSE "3rd" END')) == "1st"
        assert to_str(dbe.eval('CASE 2 WHEN 1 THEN "1st" WHEN 2 THEN "2nd" ELSE "3rd" END')) == "2nd"
        assert to_str(dbe.eval('CASE 3 WHEN 1 THEN "1st" WHEN 2 THEN "2nd" ELSE "3rd" END')) == "3rd"

        # without ELSE
        assert to_str(dbe.eval('CASE 1 WHEN 1 THEN "1st" WHEN 2 THEN "2nd" ELSE NULL END')) == "1st"
        assert to_str(dbe.eval('CASE 2 WHEN 1 THEN "1st" WHEN 2 THEN "2nd" END')) == "2nd"

    def test_case_block_returning_null(self, dbe: DbEvaluator) -> None:
        assert dbe.eval('CASE 3 WHEN 1 THEN "1st" WHEN 2 THEN "2nd" END') is None
        assert dbe.eval("CASE 3 WHEN 1 THEN 1 WHEN 2 THEN 2 END") is None
        assert dbe.eval("CASE 3 WHEN 1 THEN 1.1 WHEN 2 THEN 2.2 END") is None
        assert dbe.eval("CASE 3 WHEN 1 THEN TRUE WHEN 2 THEN TRUE END") is None
        # NULL in THEN
        assert to_str(dbe.eval('CASE 2 WHEN 1 THEN NULL WHEN 2 THEN "2nd" ELSE "3rd" END')) == "2nd"
        assert dbe.eval('CASE 1 WHEN 1 THEN NULL WHEN 2 THEN "2nd" ELSE "3rd" END') is None

    def test_case_non_const_then(self, dbe: DbEvaluator, table_schema_name: Optional[str]) -> None:
        with self.make_scalar_table(
            dbe, table_schema_name, col_name="int_value", data_type=DataType.INTEGER, value=1
        ) as scalar:
            assert dbe.eval("CASE [int_value] WHEN 1 THEN [int_value]*10 WHEN 2 THEN 8 ELSE 0 END", from_=scalar) == 10
