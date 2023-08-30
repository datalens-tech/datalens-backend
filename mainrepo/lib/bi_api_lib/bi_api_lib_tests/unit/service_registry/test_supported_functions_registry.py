import pytest

import sqlalchemy as sa

from bi_formula.definitions.functions_aggregation import AggregationFunction
from bi_formula.definitions.type_strategy import FromArgs

from bi_formula.core.datatype import DataType
from bi_formula.definitions.args import ArgTypeSequence

from bi_configs.enums import AppType

from bi_formula.definitions.base import (
    TranslationVariant,
    Function,
)
from bi_formula.core.dialect import StandardDialect as D, StandardDialect
from bi_formula.definitions.scope import Scope
from bi_formula.definitions.registry import OperationRegistry

from bi_connector_yql.formula.constants import YqlDialect

from bi_api_lib.service_registry.supported_functions_manager import _APP_TYPE_TO_SCOPE_MAP, SupportedFunctionsManager


V = TranslationVariant.make


@pytest.mark.parametrize(
    'app_type', [key for key in AppType]
)
def test_app_type_to_scope_coverage(app_type):
    assert _APP_TYPE_TO_SCOPE_MAP.get(app_type) is not None


class _TestFunction(AggregationFunction):
    scopes = Function.scopes
    name = '_test_super_func'
    variants = [
        V(D.DUMMY | YqlDialect.YQL, sa.func.sum),
    ]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER]),
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = FromArgs()


class _NonInternalFunction(_TestFunction):
    name = '_test_non_internal_func'
    scopes = _TestFunction.scopes & ~Scope.INTERNAL


@pytest.mark.parametrize(
    ('func', 'result'),
    [
        (_TestFunction(), True),
        (_NonInternalFunction(), False),
    ],
)
def test_get_supported_functions(func, result):
    registry = OperationRegistry()
    registry.register(func)
    sfm = SupportedFunctionsManager(app_type=AppType.INTRANET, operation_registry=registry)
    function_names = [f.name for f in sfm._get_supported_functions(dialect=StandardDialect.DUMMY)]
    assert (func.name in function_names) == result


@pytest.mark.parametrize(
    ('func', 'result'),
    [
        (_TestFunction(), True),
        (_NonInternalFunction(), False),
    ],
)
def test_get_supported_function_names(func, result):
    registry = OperationRegistry()
    registry.register(func)
    sfm = SupportedFunctionsManager(app_type=AppType.INTRANET, operation_registry=registry)
    function_names = sfm.get_supported_function_names(dialect=StandardDialect.DUMMY)
    assert (func.name in function_names) == result
