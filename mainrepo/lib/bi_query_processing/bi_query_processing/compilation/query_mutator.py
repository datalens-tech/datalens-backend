from __future__ import annotations

import abc
from typing import Callable, List, Sequence, Set, TypeVar

import attr

from bi_core.components.ids import FieldId

import bi_formula.core.nodes as formula_nodes
import bi_formula.core.exc as formula_exc
from bi_formula.core.dialect import DialectCombo
from bi_formula.mutation.mutation import FormulaMutation, apply_mutations
from bi_formula.mutation.bfb import NormalizeBeforeFilterByMutation
from bi_formula.mutation.tagging import FunctionLevelTagMutation
from bi_formula.mutation.window import WindowFunctionToQueryForkMutation
from bi_formula.mutation.lod import (
    ExtAggregationToQueryForkMutation, DoubleAggregationCollapsingMutation,
)
from bi_formula.mutation.lookup import LookupFunctionToQueryForkMutation
from bi_formula.mutation.general import (
    OptimizeConstComparisonMutation, OptimizeConstAndOrMutation, OptimizeUnaryBoolFunctions,
    OptimizeConstFuncMutation,
)
from bi_formula.inspect.env import InspectionEnvironment
from bi_formula.inspect.expression import (
    contains_extended_aggregations, contains_lookup_functions,
    get_toplevel_dimension_set, is_window_expression,
)
from bi_formula.validation.validator import validate
from bi_formula.validation.aggregation import AggregationChecker
from bi_formula.validation.env import ValidationEnvironment
from bi_formula.collections import NodeSet

from bi_query_processing.enums import QueryType, QueryPart
from bi_query_processing.compilation.primitives import CompiledQuery, CompiledFormulaInfo


_COMPILED_FLA_TV = TypeVar('_COMPILED_FLA_TV', bound=CompiledFormulaInfo)


def get_toplevel_dimension_set_for_query(compiled_query: CompiledQuery) -> NodeSet:
    all_dimensions = NodeSet()
    for formula in compiled_query.all_formulas:
        all_dimensions |= get_toplevel_dimension_set(formula.formula_obj.expr)
    return all_dimensions


class AtomicQueryMutatorBase(abc.ABC):
    @abc.abstractmethod
    def match_query(self, compiled_query: CompiledQuery) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def mutate_query(self, compiled_query: CompiledQuery) -> CompiledQuery:
        raise NotImplementedError


class AtomicQueryFormulaListMutatorBase(AtomicQueryMutatorBase):
    @abc.abstractmethod
    def mutate_formula_list(
            self, formula_list: List[_COMPILED_FLA_TV], query_part: QueryPart,
    ) -> List[_COMPILED_FLA_TV]:
        raise NotImplementedError

    def mutate_query(self, compiled_query: CompiledQuery) -> CompiledQuery:
        new_select = self.mutate_formula_list(compiled_query.select, query_part=QueryPart.select)
        new_group_by = self.mutate_formula_list(compiled_query.group_by, query_part=QueryPart.group_by)
        new_filters = self.mutate_formula_list(compiled_query.filters, query_part=QueryPart.filters)
        new_order_by = self.mutate_formula_list(compiled_query.order_by, query_part=QueryPart.order_by)
        new_join_on = self.mutate_formula_list(compiled_query.join_on, query_part=QueryPart.join_on)

        changed = (
            new_select is not compiled_query.select
            or new_group_by is not compiled_query.group_by
            or new_filters is not compiled_query.filters
            or new_order_by is not compiled_query.order_by
            or new_join_on is not compiled_query.join_on
        )

        if changed:
            compiled_query = compiled_query.clone(
                select=new_select, group_by=new_group_by, filters=new_filters,
                order_by=new_order_by, join_on=new_join_on,
            )

        return compiled_query


class AtomicQueryFormulaMutatorBase(AtomicQueryFormulaListMutatorBase):
    @abc.abstractmethod
    def mutate_formula(self, formula: _COMPILED_FLA_TV) -> _COMPILED_FLA_TV:
        raise NotImplementedError

    def mutate_formula_list(
            self, formula_list: List[_COMPILED_FLA_TV], query_part: QueryPart,
    ) -> List[_COMPILED_FLA_TV]:

        new_formula_list: List[_COMPILED_FLA_TV] = []
        changed = False
        for formula in formula_list:
            new_formula = self.mutate_formula(formula)
            if new_formula is not formula:
                changed = True
            new_formula_list.append(new_formula)

        if changed:
            formula_list = new_formula_list

        return formula_list


@attr.s
class IgnoreFormulaAtomicQueryMutator(AtomicQueryFormulaListMutatorBase):
    _ignore_formula_checks: Sequence[Callable[[formula_nodes.Formula, QueryPart], bool]] = attr.ib(kw_only=True)

    def match_query(self, compiled_query: CompiledQuery) -> bool:
        return True  # Apply to all

    def should_ignore_formula(self, formula: CompiledFormulaInfo, query_part: QueryPart) -> bool:
        for checker in self._ignore_formula_checks:
            if checker(formula.formula_obj, query_part):
                return True
        return False

    def mutate_formula_list(
            self, formula_list: List[_COMPILED_FLA_TV], query_part: QueryPart,
    ) -> List[_COMPILED_FLA_TV]:

        new_formula_list = [
            formula for formula in formula_list
            if not self.should_ignore_formula(formula, query_part=query_part)
        ]
        if len(new_formula_list) == len(formula_list):
            return formula_list

        return new_formula_list


@attr.s
class NullifyFormulaAtomicQueryMutator(AtomicQueryFormulaListMutatorBase):
    _nullify_formula_checks: Sequence[Callable[[formula_nodes.Formula, QueryPart], bool]] = attr.ib(kw_only=True)

    def match_query(self, compiled_query: CompiledQuery) -> bool:
        return True  # Apply to all

    def should_nullify_formula(self, formula: CompiledFormulaInfo, query_part: QueryPart) -> bool:
        for checker in self._nullify_formula_checks:
            if checker(formula.formula_obj, query_part):
                return True
        return False

    def nullify_formula(self, formula: _COMPILED_FLA_TV) -> _COMPILED_FLA_TV:
        return formula.clone(
            formula_obj=formula_nodes.Formula.make(
                expr=formula_nodes.Null(meta=formula.formula_obj.expr.meta),
                meta=formula.formula_obj.meta,
            )
        )

    def mutate_formula_list(
            self, formula_list: List[_COMPILED_FLA_TV], query_part: QueryPart,
    ) -> List[_COMPILED_FLA_TV]:

        new_formula_list = [
            self.nullify_formula(formula)
            if self.should_nullify_formula(formula, query_part=query_part)
            else formula
            for formula in formula_list
        ]
        return new_formula_list


def contains_inconsistent_aggregations(
        node: formula_nodes.FormulaItem,
        dimensions: list[formula_nodes.FormulaItem],
        env: ValidationEnvironment,
) -> bool:
    try:
        validate(
            node, env=env,
            checkers=[
                AggregationChecker(
                    valid_env=env,
                    inspect_env=InspectionEnvironment(),
                    global_dimensions=dimensions
                )
            ],
            collect_errors=False
        )
    except formula_exc.ValidationError:
        return True

    return False


@attr.s
class DefaultAtomicQueryMutator(AtomicQueryFormulaMutatorBase):
    _mutations: Sequence[FormulaMutation] = attr.ib(kw_only=True)

    def match_query(self, compiled_query: CompiledQuery) -> bool:
        return True  # Apply to all

    def mutate_formula(self, formula: _COMPILED_FLA_TV) -> _COMPILED_FLA_TV:
        formula_obj = apply_mutations(tree=formula.formula_obj, mutations=self._mutations)
        if formula_obj is formula.formula_obj:
            # No changes
            return formula
        formula = formula.clone(formula_obj=formula_obj)
        return formula


def formula_is_true(formula: formula_nodes.Formula, query_part: QueryPart) -> bool:
    return (
        query_part == QueryPart.filters
        and isinstance(formula.expr, formula_nodes.LiteralBoolean)
        and formula.expr.value is True
    )


@attr.s
class QueryMutator(abc.ABC):
    """
    Applies mutations to formulas of the query after it has been more or less assembled.
    They are applied conditionally after the whole query is examined (or not).
    """

    @abc.abstractmethod
    def mutate_query(self, compiled_query: CompiledQuery) -> CompiledQuery:
        raise NotImplementedError


@attr.s
class OptimizingQueryMutator(QueryMutator):
    """Mutator applies various optimizations and (in specific cases) "fixes" to the query"""

    _dialect: DialectCombo = attr.ib(kw_only=True)
    _disable_optimizations: bool = attr.ib(kw_only=True, default=False)

    def mutate_query(self, compiled_query: CompiledQuery) -> CompiledQuery:
        # Apply "preliminary" mutation
        # (must be applied before we scan for nested aggregations)
        if not self._disable_optimizations:
            mutator: AtomicQueryMutatorBase = DefaultAtomicQueryMutator(mutations=[
                DoubleAggregationCollapsingMutation(),
                OptimizeConstComparisonMutation(),
                OptimizeConstAndOrMutation(),
                OptimizeUnaryBoolFunctions(self._dialect),
                OptimizeConstFuncMutation(),
            ])
            compiled_query = mutator.mutate_query(compiled_query)

        filter_mutator = IgnoreFormulaAtomicQueryMutator(ignore_formula_checks=[formula_is_true])
        compiled_query = filter_mutator.mutate_query(compiled_query)

        has_extaggs = any(
            contains_extended_aggregations(node=formula.formula_obj, include_double_agg=True)
            for formula in compiled_query.all_formulas
        )

        # Disable all the query forking and window functions for (sub)totals.
        if compiled_query.meta.query_type == QueryType.totals:
            validation_env = ValidationEnvironment()
            inspect_env = InspectionEnvironment()
            dimensions = [
                formula.formula_obj.expr for formula in compiled_query.group_by
            ]
            has_incaggs = any(
                contains_inconsistent_aggregations(
                    node=formula.formula_obj, dimensions=dimensions, env=validation_env,
                )
                for formula in compiled_query.all_formulas
            )
            has_winfuncs = any(
                is_window_expression(node=formula.formula_obj, env=inspect_env)
                for formula in compiled_query.all_formulas
            )

            if has_extaggs or has_incaggs or has_winfuncs:
                null_mutator = NullifyFormulaAtomicQueryMutator(
                    nullify_formula_checks=[
                        lambda f, qpart: contains_lookup_functions(f),
                        lambda f, qpart: contains_extended_aggregations(f, include_double_agg=True),
                        lambda f, qpart: contains_inconsistent_aggregations(
                            f, dimensions=dimensions, env=validation_env,
                        ),
                        lambda f, qpart: is_window_expression(f, env=inspect_env),
                    ]
                )
                compiled_query = null_mutator.mutate_query(compiled_query)

        return compiled_query


@attr.s
class ExtendedAggregationQueryMutator(QueryMutator):
    """Mutator specifically for handling LODs and lookup functions"""

    _allow_empty_dimensions_for_forks: bool = attr.ib(kw_only=True)
    _allow_arbitrary_toplevel_lod_dimensions: bool = attr.ib(kw_only=True)
    _new_subquery_mode: bool = attr.ib(kw_only=True, default=False)

    def mutate_query(self, compiled_query: CompiledQuery) -> CompiledQuery:
        global_dimensions = [formula.formula_obj.expr for formula in compiled_query.group_by]

        filter_ids: Set[FieldId] = set()
        for formula in compiled_query.filters:
            if formula.original_field_id:
                filter_ids.add(formula.original_field_id)
            # Otherwise it is an anonymous filter (RLS) that cannot interact with BFB

        has_extaggs = any(
            contains_extended_aggregations(node=formula.formula_obj, include_double_agg=True)
            for formula in compiled_query.all_formulas
        )

        has_lookups = any(
            contains_lookup_functions(node=formula.formula_obj)
            for formula in compiled_query.all_formulas
        )

        will_mutate_winfuncs = False
        if self._new_subquery_mode:
            inspect_env = InspectionEnvironment()
            has_winfuncs = any(
                is_window_expression(node=formula.formula_obj, env=inspect_env)
                for formula in compiled_query.all_formulas
            )
            will_mutate_winfuncs = has_winfuncs

        fork_mutators: List[AtomicQueryMutatorBase] = []

        if has_lookups:
            fork_mutators += [
                DefaultAtomicQueryMutator(mutations=[
                    LookupFunctionToQueryForkMutation(
                        global_dimensions=global_dimensions,
                        allow_empty_dimensions=self._allow_empty_dimensions_for_forks,
                    ),
                ]),
            ]

        if has_extaggs or will_mutate_winfuncs:
            # Window functions also require aggregations to be wrapped in QF
            # so that they can be split-off to sub-queries separate from the ones with the WF
            fork_mutators += [
                DefaultAtomicQueryMutator(mutations=[
                    ExtAggregationToQueryForkMutation(global_dimensions=global_dimensions),
                ]),
            ]

        if will_mutate_winfuncs:
            fork_mutators += [
                DefaultAtomicQueryMutator(mutations=[
                    WindowFunctionToQueryForkMutation(global_dimensions=global_dimensions),
                ]),
            ]

        atomic_mutators: list[AtomicQueryMutatorBase]
        if not self._new_subquery_mode:
            atomic_mutators = [
                # BFB clause preparation
                DefaultAtomicQueryMutator(mutations=[
                    NormalizeBeforeFilterByMutation(available_filter_field_ids=filter_ids),
                ]),

                # Fork mutations
                *fork_mutators,

                # Tagging must be done strictly after the previous mutations have been applied to all nodes,
                DefaultAtomicQueryMutator(mutations=[
                    FunctionLevelTagMutation(),
                ]),
            ]
        else:
            atomic_mutators = fork_mutators

        for mutator in atomic_mutators:
            if mutator.match_query(compiled_query):
                compiled_query = mutator.mutate_query(compiled_query)

        if has_extaggs and not self._allow_arbitrary_toplevel_lod_dimensions:
            dim_set_from_group_by = NodeSet(global_dimensions)
            dim_set_from_top_lods = get_toplevel_dimension_set_for_query(compiled_query)
            if (dim_set_from_group_by | dim_set_from_top_lods) != dim_set_from_group_by:
                # LODs contain some dimensions that are not in `group_by`
                raise formula_exc.LodInvalidTopLevelDimensionsError(
                    'Invalid top-level LOD dimension found in expression'
                )

        if has_extaggs and not self._new_subquery_mode:
            # The real group_by's of each sub-query will be managed by forker
            # while it is inspecting QueryFork's.
            # The original group_by can be removed because they will only get in the way
            compiled_query = compiled_query.clone(group_by=[])

        return compiled_query
