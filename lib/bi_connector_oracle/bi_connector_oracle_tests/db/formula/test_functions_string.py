import os

import sqlalchemy as sa

from dl_formula_testing.testcases.functions_string import (
    DefaultStringFunctionFormulaConnectorTestSuite,
)
from dl_formula_testing.evaluator import DbEvaluator

from bi_connector_oracle_tests.db.formula.base import (
    OracleTestBase,
)


class TestStringFunctionOracle(OracleTestBase, DefaultStringFunctionFormulaConnectorTestSuite):
    supports_split_3 = False

    def test_oracle_unicode(self, dbe: DbEvaluator) -> None:
        assert os.environ.get('NLS_LANG') == '.AL32UTF8'
        # assert dbe.db.scalar(sa.select([sa.literal('карл')])) == 'карл'  # not working.
        assert dbe.db.scalar(sa.literal('карл', type_=sa.sql.sqltypes.NCHAR())) == 'карл'
