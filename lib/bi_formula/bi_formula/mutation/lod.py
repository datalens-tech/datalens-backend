from __future__ import annotations

from typing import List, Tuple

import attr

import bi_formula.core.exc as exc
import bi_formula.core.nodes as nodes
import bi_formula.core.aux_nodes as aux_nodes
import bi_formula.core.fork_nodes as fork_nodes
from bi_formula.mutation.mutation import FormulaMutation
from bi_formula.mutation.dim_resolution import DimensionResolvingMutationBase
from bi_formula.inspect.node import is_aggregate_function, qfork_is_aggregation
from bi_formula.inspect.expression import is_aggregate_expression
from bi_formula.shortcuts import n


@attr.s
class ExtAggregationToQueryForkMutation(DimensionResolvingMutationBase):
    """
    A mutation that adds ``QueryFork`` nodes for all aggregations
    for future LOD handling.
    """

    def match_node(
            self, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]
    ) -> bool:
        is_agg = is_aggregate_function(node)
        direct_parent = parent_stack[-1]
        already_patched = (
            isinstance(direct_parent, fork_nodes.QueryFork)
            and direct_parent.result_expr is node
            and qfork_is_aggregation(direct_parent)  # is an aggregation fork (vs. AGO forks)
        )
        return is_agg and not already_patched

    def make_replacement(
            self, old: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.FuncCall)

        dimensions, _, parent_dimension_set = self._generate_dimensions(node=old, parent_stack=parent_stack)
        lod = nodes.FixedLodSpecifier.make(dim_list=dimensions)

        condition_list: List[fork_nodes.JoinConditionBase] = []
        for dimension_expr in dimensions:
            if is_aggregate_expression(dimension_expr, env=self._inspect_env):
                return aux_nodes.ErrorNode.make(
                    err_code=exc.LodMeasureDimensionsError.default_code,
                    message='A measure has been used as a LOD dimension.',
                    meta=old.meta,
                )
            dim_condition = fork_nodes.SelfEqualityJoinCondition.make(expr=dimension_expr)
            condition_list.append(dim_condition)

        joining = fork_nodes.QueryForkJoiningWithList.make(condition_list=condition_list)

        # Replace lod in the original function
        old_updated = nodes.FuncCall.make(
            name=old.name,
            args=old.args,
            ignore_dimensions=old.ignore_dimensions,
            before_filter_by=old.before_filter_by,
            lod=lod,
            meta=old.meta,
        )

        return fork_nodes.QueryFork.make(
            join_type=fork_nodes.JoinType.inner,
            result_expr=old_updated,
            joining=joining,
            lod=lod,
            before_filter_by=old.before_filter_by,
            meta=old.meta,
        )


@attr.s
class DoubleAggregationCollapsingMutation(FormulaMutation):
    """
    A mutation removes double aggregations where possible.
    """
    # TODO: Add warnings when such optimizations are made

    _AGG_TRANSFORM_MAP = {
        # <parent agg> -> what to replace with
        'sum': lambda child, meta: child,
        'any': lambda child, meta: child,
        'max': lambda child, meta: child,
        'min': lambda child, meta: child,
        'count': lambda child, meta: n.func.INT(n.not_(n.func.ISNULL(child, meta=meta), meta=meta), meta=meta),
        'countd': lambda child, meta: n.func.INT(n.not_(n.func.ISNULL(child, meta=meta), meta=meta), meta=meta),
        # 'min' is a bit tricky (converts int -> float)
    }

    def match_node(
            self, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]
    ) -> bool:
        """
        Match if node is agg, and "main" argument is also an agg
        and they have the same LOD and BFB.
        """

        if not is_aggregate_function(node):
            return False
        assert isinstance(node, nodes.FuncCall)

        if node.name not in self._AGG_TRANSFORM_MAP:
            # No rule for this aggregation, so abort
            return False

        if len(node.args) != 1:
            # No child to inspect, or too many of them
            return False

        child_agg_node = node.args[0]
        if not is_aggregate_function(child_agg_node):
            # Child is not agg
            return False
        assert isinstance(child_agg_node, nodes.FuncCall)

        if child_agg_node.lod != node.lod or child_agg_node.before_filter_by != node.before_filter_by:
            # Mismatch of advanced params
            return False

        return True

    def make_replacement(
            self, old: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.FuncCall)

        child_agg_node = old.args[0]

        node_transform = self._AGG_TRANSFORM_MAP[old.name]
        new_node = node_transform(child_agg_node, old.meta)

        assert isinstance(new_node, nodes.FormulaItem)
        return new_node
