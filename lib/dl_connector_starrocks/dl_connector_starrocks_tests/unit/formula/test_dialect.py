from dl_formula_testing.testcases.dialect import DefaultDialectFormulaConnectorTestSuite

from dl_connector_starrocks.formula.constants import DIALECT_NAME_STARROCKS
from dl_connector_starrocks.formula.constants import StarRocksDialect as D


class TestDialectStarRocks(DefaultDialectFormulaConnectorTestSuite):
    dialect_name = DIALECT_NAME_STARROCKS
    default_dialect = D.STARROCKS_3_0
    dialect_matches = (
        ("3.0.0", D.STARROCKS_3_0),
        ("3.1.5", D.STARROCKS_3_0),
        ("3.3.0", D.STARROCKS_3_0),
    )
