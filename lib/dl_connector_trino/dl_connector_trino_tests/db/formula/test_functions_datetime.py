from dl_formula_testing.testcases.functions_datetime import DefaultDateTimeFunctionFormulaConnectorTestSuite

from dl_connector_trino_tests.db.formula.base import TrinoFormulaTestBase


class TestDateTimeFunctionTrino(TrinoFormulaTestBase, DefaultDateTimeFunctionFormulaConnectorTestSuite):
    supports_dateadd_non_const_unit_num = True
    supports_deprecated_dateadd = True
    supports_deprecated_datepart_2 = True
    supports_datetrunc_3 = False  # May be supported in the future
    supports_datetimetz = True
