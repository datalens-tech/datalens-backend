import pytest
import sqlalchemy as sa

import dl_api_lib.query.registry
from dl_api_lib.service_registry.supported_functions_manager import SupportedFunctionsManager
from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import StandardDialect
from dl_formula.core.dialect import StandardDialect as D
from dl_formula.definitions.args import ArgTypeSequence
from dl_formula.definitions.base import (
    Function,
    TranslationVariant,
)
from dl_formula.definitions.functions_aggregation import AggregationFunction
from dl_formula.definitions.registry import OperationRegistry
from dl_formula.definitions.scope import Scope
from dl_formula.definitions.type_strategy import FromArgs
from dl_formula.mutation.registry import get_mutation_lookup_functions_names

V = TranslationVariant.make


LOOKUP_FUNCTIONS = get_mutation_lookup_functions_names()


class _TestFunction(AggregationFunction):
    scopes = Function.scopes
    name = "_test_super_func"
    variants = [
        V(D.DUMMY, sa.func.sum),
    ]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER]),
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = FromArgs()


class _NonStableFunction(_TestFunction):
    name = "_test_non_stable_func"
    scopes = _TestFunction.scopes & ~Scope.STABLE


def test_lookup_functions_names():
    assert LOOKUP_FUNCTIONS == ["ago", "at_date"]


@pytest.mark.parametrize(
    ("func", "result"),
    [
        (_TestFunction(), True),
        (_NonStableFunction(), False),
    ],
)
def test_get_supported_functions(func, result):
    registry = OperationRegistry()
    registry.register(func)
    sfm = SupportedFunctionsManager(supported_tags=("stable",), operation_registry=registry)
    functions = sfm._get_supported_functions(dialect=StandardDialect.DUMMY)
    function_names = sfm.get_supported_function_names(dialect=StandardDialect.DUMMY)

    assert [f.name for f in functions] + LOOKUP_FUNCTIONS == function_names
    assert (func.name in function_names) == result


@pytest.mark.parametrize(
    ("is_forkable", "result"),
    [
        (True, LOOKUP_FUNCTIONS),
        (False, []),
    ],
)
def test_get_supported_lookup_function_names(is_forkable, result, monkeypatch):
    dialect = StandardDialect.DUMMY
    monkeypatch.setattr(
        dl_api_lib.query.registry,
        "_IS_FORKABLE_DIALECT",
        dl_api_lib.query.registry._IS_FORKABLE_DIALECT | {dialect.common_name: is_forkable},
    )

    registry = OperationRegistry()
    sfm = SupportedFunctionsManager(supported_tags=("stable",), operation_registry=registry)

    function_names = sfm.get_supported_function_names(dialect=dialect)
    assert function_names == result
