from dl_formula_testing.testcases.conditional_blocks import DefaultConditionalBlockFormulaConnectorTestSuite

from dl_connector_postgresql_tests.db.formula.base import (
    PostgreSQL_9_3TestBase,
    PostgreSQL_9_4TestBase,
)
from dl_testing.regulated_test import RegulatedTestParams


class TestConditionalBlockPostgreSQL_9_3(PostgreSQL_9_3TestBase, DefaultConditionalBlockFormulaConnectorTestSuite):
     test_params = RegulatedTestParams(
        mark_tests_skipped={
            DefaultConditionalBlockFormulaConnectorTestSuite.test_if_block: "lalal",
        }
    )
    # pass



class TestConditionalBlockPostgreSQL_9_4(PostgreSQL_9_4TestBase, DefaultConditionalBlockFormulaConnectorTestSuite):
    pass
