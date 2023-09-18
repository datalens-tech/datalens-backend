from dl_connector_clickhouse.formula.constants import ClickHouseDialect
from dl_formula.definitions.scope import Scope
from dl_formula_ref.categories.aggregation import CATEGORY_AGGREGATION
from dl_formula_ref.categories.logical import CATEGORY_LOGICAL
from dl_formula_ref.categories.mathematical import CATEGORY_MATHEMATICAL
from dl_formula_ref.categories.time_series import CATEGORY_TIME_SERIES
from dl_formula_ref.categories.window import CATEGORY_WINDOW
from dl_formula_ref.registry.env import GenerationEnvironment
from dl_formula_ref.registry.registry import (
    FUNC_REFERENCE_REGISTRY,
    RefFunctionKey,
)
from dl_formula_ref.registry.tools import populate_registry_from_definitions


def check_function(func_name: str, exp_signature: list, category_name: str) -> None:
    func_key = RefFunctionKey.normalized(name=func_name, category_name=category_name)
    func = FUNC_REFERENCE_REGISTRY[func_key]
    env = GenerationEnvironment(
        scopes=Scope.DOCUMENTED,
        supported_dialects=frozenset({ClickHouseDialect.CLICKHOUSE_22_10}),
    )
    signatures = [sig.body for sig in func.get_signatures(env=env).signatures]
    assert signatures == exp_signature


def test_signatures():
    populate_registry_from_definitions()
    # Function without args
    check_function("PI", ["PI()"], CATEGORY_MATHEMATICAL.name)
    # Function with one arg
    check_function("ABS", ["ABS( number )"], CATEGORY_MATHEMATICAL.name)
    # Function with multiple args
    check_function("IFNULL", ["IFNULL( check_value, alt_value )"], CATEGORY_LOGICAL.name)
    # Function with optional args
    check_function("ROUND", ["ROUND( number [ , precision ] )"], CATEGORY_MATHEMATICAL.name)
    # Infinite arg function
    check_function("GREATEST", ["GREATEST( value_1, value_2, value_3 [ , ... ] )"], CATEGORY_MATHEMATICAL.name)
    # Aggregation
    check_function(
        "SUM",
        [
            "SUM( value )",
            "SUM( value\n" "     [ FIXED ... | INCLUDE ... | EXCLUDE ... ]\n" "     [ BEFORE FILTER BY ... ]\n" "   )",
        ],
        CATEGORY_AGGREGATION.name,
    )
    # Window function without ORDER BY
    check_function(
        "SUM",
        [
            "SUM( value\n" "     TOTAL | WITHIN ... | AMONG ...\n" "   )",
            "SUM( value\n" "     TOTAL | WITHIN ... | AMONG ...\n" "     [ BEFORE FILTER BY ... ]\n" "   )",
        ],
        CATEGORY_WINDOW.name,
    )
    # Window function with ORDER BY
    check_function(
        "MSUM",
        [
            "MSUM( value, rows_1 [ , rows_2 ] )",
            "MSUM( value, rows_1 [ , rows_2 ]\n"
            "      [ TOTAL | WITHIN ... | AMONG ... ]\n"
            "      [ ORDER BY ... ]\n"
            "      [ BEFORE FILTER BY ... ]\n"
            "    )",
        ],
        CATEGORY_WINDOW.name,
    )
    # Lookup function
    check_function(
        "AGO",
        [
            "AGO( measure, date_dimension [ , unit [ , number ] ] )",
            "AGO( measure, date_dimension [ , unit [ , number ] ]\n"
            "     [ BEFORE FILTER BY ... ]\n"
            "     [ IGNORE DIMENSIONS ... ]\n"
            "   )",
        ],
        CATEGORY_TIME_SERIES.name,
    )
