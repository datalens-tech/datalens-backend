from bi_formula.connectors.base.testing.dialect import DefaultDialectFormulaConnectorTestSuite

from bi_connector_clickhouse.formula.constants import ClickHouseDialect as D, DIALECT_NAME_CLICKHOUSE


class DialectClickHouseTestSuite(DefaultDialectFormulaConnectorTestSuite):
    dialect_name = DIALECT_NAME_CLICKHOUSE
    default_dialect = D.CLICKHOUSE_21_8
    dialect_matches = (
        ('21.9.10', D.CLICKHOUSE_21_8),
        ('23.1.3', D.CLICKHOUSE_22_10),
    )
