from __future__ import annotations

from typing import (
    List,
    Sequence,
    Tuple,
)

import attr

import bi_formula.core.exc as exc
import bi_formula.core.fork_nodes as fork_nodes
import bi_formula.core.nodes as nodes
from bi_formula.inspect.expression import (
    is_aggregate_expression,
    is_bound_only_to,
)
from bi_formula.inspect.function import uses_default_ordering
from bi_formula.inspect.node import qfork_is_window
from bi_formula.mutation.dim_resolution import DimensionResolvingMutationBase
from bi_formula.mutation.mutation import FormulaMutation


class AmongToWithinGroupingMutation(DimensionResolvingMutationBase):
    """
    A mutation that converts all (relative) ``WITHIN`` window function groupings
    to (absolute) ``AMONG`` groupings.
    """

    def match_node(self, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]) -> bool:
        return isinstance(node, nodes.WindowGroupingAmong)

    def make_replacement(
        self, old: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.WindowGroupingAmong)
        dimensions, _, _ = self._generate_dimensions(node=old, parent_stack=parent_stack)
        new_dimensions = dimensions[:]
        for dimension in old.dim_list:
            for i in range(len(new_dimensions)):
                if new_dimensions[i].extract == dimension.extract:
                    del new_dimensions[i]
                    break
            else:
                raise exc.UnknownWindowDimensionError("Unknown dimension for window")

        return nodes.WindowGroupingWithin.make(dim_list=new_dimensions, meta=old.meta)


@attr.s
class IgnoreExtraWithinGroupingMutation(DimensionResolvingMutationBase):
    """
    A mutation that ignores (removes) all expressions from the``WITHIN`` clause
    that are not listed among dimensions or are not aggregate expressions.
    """

    def match_node(self, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]) -> bool:
        return isinstance(node, nodes.WindowGroupingWithin)

    def make_replacement(
        self, old: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.WindowGroupingWithin)
        _, dimension_set, _ = self._generate_dimensions(node=old, parent_stack=parent_stack)
        new_dimensions: List[nodes.FormulaItem] = [
            dimension
            for dimension in old.dim_list
            if (
                # Allow only known dimensions
                is_bound_only_to(node=dimension, allow_nodes=dimension_set)
                # and aggregate expressions.
                or is_aggregate_expression(node=dimension, env=self._inspect_env)
                # Ignore everything else.
            )
        ]
        return nodes.WindowGroupingWithin.make(dim_list=new_dimensions, meta=old.meta)


class DefaultWindowOrderingMutation(FormulaMutation):
    """
    A mutation that adds default ordering to all window function calls.

    If a function is not ordered before the mutation,
    then add the default ordering to it.
    If a function is already ordered, then add items
    from the default ordering that are not present in the function's ordering.
    """

    def __init__(self, default_order_by: Sequence[nodes.FormulaItem]):
        self._default_order_by = list(default_order_by)

    def match_node(self, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]) -> bool:
        return isinstance(node, nodes.WindowFuncCall) and uses_default_ordering(node.name)

    def make_replacement(
        self, old: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.WindowFuncCall)

        existing_order_by = list(old.ordering.expr_list)
        if not existing_order_by:
            # Node has no ordering of its own, so add it as-is
            new_order_by_list = self._default_order_by

        else:
            # For nodes that are already ordered patch orderings with items they don't contain
            # (ignore ASC/DESC when comparing ORDER BY items)

            def _unwrap_order_by_item(node: nodes.FormulaItem) -> nodes.FormulaItem:
                if isinstance(node, nodes.OrderingDirectionBase):
                    node = node.expr
                return node

            existing_order_by_extracts = [_unwrap_order_by_item(expr).extract for expr in existing_order_by]
            new_order_by_list = [n for n in old.ordering.expr_list]
            for default_order_item in self._default_order_by:
                if _unwrap_order_by_item(default_order_item).extract not in existing_order_by_extracts:
                    new_order_by_list.append(default_order_item)

        return nodes.WindowFuncCall.make(
            name=old.name,
            args=old.args,
            ordering=nodes.Ordering.make(expr_list=new_order_by_list),
            grouping=old.grouping,
            before_filter_by=old.before_filter_by,
            ignore_dimensions=old.ignore_dimensions,
            lod=old.lod,
            meta=old.meta,
        )


class WindowFunctionToQueryForkMutation(DimensionResolvingMutationBase):
    """
    A mutation that wraps all window functions into `QueryFork` nodes
    """

    def match_node(self, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]) -> bool:
        is_winfunc = isinstance(node, nodes.WindowFuncCall)
        direct_parent = parent_stack[-1]
        already_patched = (
            isinstance(direct_parent, fork_nodes.QueryFork)
            and direct_parent.result_expr is node
            and qfork_is_window(direct_parent)
        )
        return is_winfunc and not already_patched

    def make_replacement(
        self, old: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.WindowFuncCall)

        dimensions, _, parent_dimension_set = self._generate_dimensions(node=old, parent_stack=parent_stack)
        lod = nodes.FixedLodSpecifier.make(dim_list=dimensions)

        condition_list: List[fork_nodes.JoinConditionBase] = []
        for dimension_expr in dimensions:
            dim_condition = fork_nodes.SelfEqualityJoinCondition.make(expr=dimension_expr)
            condition_list.append(dim_condition)

        joining = fork_nodes.QueryForkJoiningWithList.make(condition_list=condition_list)

        return fork_nodes.QueryFork.make(
            join_type=fork_nodes.JoinType.inner,
            result_expr=old,
            joining=joining,
            lod=lod,
            before_filter_by=old.before_filter_by,
            meta=old.meta,
        )
