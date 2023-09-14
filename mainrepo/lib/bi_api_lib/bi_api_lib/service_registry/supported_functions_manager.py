from functools import reduce
from itertools import chain
from operator import ior

import attr

from bi_api_lib.enums import (
    BI_TYPE_AGGREGATIONS,
    FILTERS_BY_TYPE,
)
from bi_api_lib.query.registry import (
    get_compeng_dialect,
    is_compeng_enabled,
)
from bi_constants.enums import (
    AggregationFunction,
    BIType,
    WhereClauseOperation,
)
from bi_formula.core.dialect import DialectCombo
import bi_formula.core.nodes as formula_nodes
from bi_formula.definitions.registry import (
    OPERATION_REGISTRY,
    FuncKey,
    OperationRegistry,
)
from bi_formula.definitions.scope import Scope
from bi_formula.inspect.expression import iter_operation_calls
from bi_query_processing.compilation.filter_compiler import FilterFormulaCompiler
from bi_query_processing.compilation.formula_compiler import FormulaCompiler
from bi_utils.func_tools import method_lru

_FUNCTION_TAG_TO_SCOPE_MAP: dict[str, Scope] = {
    "stable": Scope.STABLE,
}


@attr.s(frozen=True)
class SupportedFunctionsManager:
    """
    Service-helper.
    Translate env type to formula's format and provide some wrappers for get_supported_functions.
    """

    _supported_tags: tuple[str] = attr.ib()
    _operation_registry: OperationRegistry = attr.ib(default=OPERATION_REGISTRY)

    @method_lru(maxsize=1000)
    def get_supported_filters(self, dialect: DialectCombo, user_type: BIType) -> list[WhereClauseOperation]:
        return [
            op
            for op in self._get_supported_filters_for_dialect(dialect=dialect)
            if op in FILTERS_BY_TYPE.get(user_type, [])
        ]

    @method_lru(maxsize=1000)
    def get_supported_aggregations(self, dialect: DialectCombo, user_type: BIType) -> list[AggregationFunction]:
        supported_func_names = set(
            name
            for name, *_ in self._get_supported_functions(
                dialect=dialect,
                scope=Scope.EXPLICIT_USAGE,
            )
        )

        return [
            ag_type
            for ag_type in BI_TYPE_AGGREGATIONS[user_type]
            # TODO: make _agg_functions public?
            if FormulaCompiler._agg_functions.get(ag_type, ag_type.name) in supported_func_names
        ]

    def get_supported_function_names(self, dialect: DialectCombo) -> list[str]:
        native_supp_funcs = self._get_supported_functions(
            dialect=dialect,
            scope=Scope.SUGGESTED,
        )

        if is_compeng_enabled():
            compeng_supp_funcs = self._get_supported_functions(
                dialect=get_compeng_dialect(),
                scope=Scope.SUGGESTED,
            )
        else:
            compeng_supp_funcs = []

        supported_function_names = sorted(
            {
                func[0]
                for func in chain.from_iterable(
                    (
                        native_supp_funcs,
                        compeng_supp_funcs,
                    )
                )
            }
        )
        return supported_function_names

    @property
    def _supported_scopes(self) -> tuple[int, ...]:
        return tuple(_FUNCTION_TAG_TO_SCOPE_MAP[tag].value for tag in self._supported_tags)

    @method_lru(maxsize=1000)
    def _get_supported_functions(
        self,
        dialect: DialectCombo,
        scope: int = Scope.EXPLICIT_USAGE,
        only_functions: bool = True,
    ) -> list[FuncKey]:
        return self._operation_registry.get_supported_functions(
            require_dialects=dialect,
            only_functions=only_functions,
            function_scopes=scope | reduce(ior, self._supported_scopes),
        )

    @method_lru(maxsize=20)
    def _get_supported_filters_for_dialect(self, dialect: DialectCombo) -> list[WhereClauseOperation]:
        result = []
        supported_funcs = {
            (name, arg_cnt)
            for name, arg_cnt, *_ in self._get_supported_functions(
                dialect=dialect,
                only_functions=False,
                scope=Scope.EXPLICIT_USAGE,
            )
        }
        for filter_op, filter_def in FilterFormulaCompiler.FILTER_OPERATIONS.items():
            arg_cnt = filter_def.arg_cnt
            expr_factory = filter_def.callable
            if arg_cnt is None:
                # args are interpreted as a single list
                # and the field itself is the first argument
                arg_cnt = 2
            else:
                arg_cnt += 1  # The field itself is also an argument

            expr = expr_factory(*(formula_nodes.Null() for _ in range(arg_cnt)))
            for op_node in iter_operation_calls(expr):
                func_key = (op_node.name, len(op_node.args))
                if func_key in supported_funcs:
                    # FIXME: type check
                    result.append(filter_op)
        return result
