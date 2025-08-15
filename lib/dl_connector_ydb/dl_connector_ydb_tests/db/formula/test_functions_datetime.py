import ydb_sqlalchemy as ydb_sa

from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_datetime import DefaultDateTimeFunctionFormulaConnectorTestSuite

from dl_connector_ydb_tests.db.formula.base import YQLTestBase


class TestDateTimeFunctionYQL(YQLTestBase, DefaultDateTimeFunctionFormulaConnectorTestSuite):
    supports_datepart_2_non_const = False

    # TODO(catsona): Remove these tests and freeze expected behavior of custom dialect
    def test_datetime_precision(self, dbe: DbEvaluator) -> None:
        # ydb_sqlalchemy DateTime is mapped to YDB Timestamp type,
        #  while database also has a DateTime type with different precision.
        # These types behave different:
        # - CAST(DateTime as Int64) produces seconds
        # - CAST(Timestamp as Int64) produces microseconds

        assert dbe.db.execute("SELECT CAST(CAST(DateTime::FromSeconds(1) AS Timestamp) AS Int64)").scalar() == 1000000
        assert dbe.db.execute("SELECT CAST(CAST(DateTime::FromSeconds(1) AS DateTime) AS Int64)").scalar() == 1

    def test_ydb_sa_datetime_native_type(self, dbe: DbEvaluator) -> None:
        # Check expected behavior of sa.DateTime, see test_datetime_precision

        assert ydb_sa.types.YqlTimestamp.__visit_name__ == "TIMESTAMP"
        assert ydb_sa.types.YqlDateTime.__visit_name__ == "TIMESTAMP"
