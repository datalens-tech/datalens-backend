from bi_formula.core.dialect import DialectName
from bi_formula.connectors.base.testing.dialect import DefaultDialectFormulaConnectorTestSuite

from bi_connector_clickhouse.formula.constants import ClickHouseDialect as D


class DialectClickHouseTestSuite(DefaultDialectFormulaConnectorTestSuite):
    dialect_name = DialectName.CLICKHOUSE
    default_dialect = D.CLICKHOUSE_21_8
    dialect_matches = (
        ('21.9.10', D.CLICKHOUSE_21_8),
        ('23.1.3', D.CLICKHOUSE_22_10),
    )
