# import pytest
# import sqlalchemy as sa

# from dl_formula_testing.evaluator import DbEvaluator
from dl_formula_testing.testcases.functions_hash import DefaultHashFunctionFormulaConnectorTestSuite

from dl_connector_clickhouse_tests.db.formula.base import (
    ClickHouse_21_8TestBase,
    ClickHouse_22_10TestBase,
)


class TestHashFunctionClickHouse_21_8(
    ClickHouse_21_8TestBase,
    DefaultHashFunctionFormulaConnectorTestSuite,
):
    pass


class TestHashFunctionClickHouse_22_10(
    ClickHouse_22_10TestBase,
    DefaultHashFunctionFormulaConnectorTestSuite,
):
    pass
