import datetime

from dl_formula_testing.testcases.literals import DefaultLiteralFormulaConnectorTestSuite

from bi_connector_mssql_tests.db.formula.base import MSSQLTestBase


class TestConditionalBlockMSSQL(MSSQLTestBase, DefaultLiteralFormulaConnectorTestSuite):
    recognizes_datetime_type = False  # SQLAlchemy does not recognize DATETIMEOFFSET as a datetime.datetime
    supports_microseconds = True
    supports_utc = True
    supports_custom_tz = True
    default_tz = datetime.timezone.utc
