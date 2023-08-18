from __future__ import annotations

from typing import Optional

import bi_formula.core.nodes as nodes
import bi_formula.core.fork_nodes as fork_nodes
from bi_formula.inspect.function import can_be_aggregate, supports_bfb


def get_token(node: nodes.FormulaItem) -> Optional[str]:
    if isinstance(node, nodes.NamedItem):
        return node.name
    if isinstance(node, (nodes.LiteralUuid, nodes.LiteralString, nodes.LiteralBoolean, nodes.LiteralInteger,
                         nodes.LiteralFloat, nodes.LiteralGeopoint, nodes.LiteralGeopolygon)):
        return str(node.value)
    if isinstance(node, nodes.Null):
        return 'null'
    return None


def is_aggregate_function(node: nodes.FormulaItem) -> bool:
    return (
        isinstance(node, nodes.FuncCall)
        and not isinstance(node, nodes.WindowFuncCall)
        and can_be_aggregate(node.name)
    )


def is_window_function(node: nodes.FormulaItem) -> bool:
    return isinstance(node, nodes.WindowFuncCall)


def is_lookup_function(node: nodes.FormulaItem) -> bool:
    # FIXME: Add a separate function attribute for that
    if isinstance(node, nodes.FuncCall) and not isinstance(node, nodes.WindowFuncCall):
        if not can_be_aggregate(node.name) and supports_bfb(node.name, is_window=False):
            return True

    return False


def has_non_default_lod_dimensions(node: nodes.FormulaItem) -> bool:
    if isinstance(node, nodes.FuncCall):
        if is_aggregate_function(node):
            if not isinstance(node.lod, nodes.DefaultAggregationLodSpecifier):
                # Aggregations with custom LODs
                return True

    return False


def is_extended_aggregation(node: nodes.FormulaItem) -> bool:
    if isinstance(node, nodes.FuncCall):
        if is_aggregate_function(node):
            if not isinstance(node.lod, nodes.DefaultAggregationLodSpecifier):
                # Aggregations with custom LODs
                return True
            if node.before_filter_by.field_names:
                return True

    return False


def is_default_lod_aggregation(node: nodes.FormulaItem) -> bool:
    if is_aggregate_function(node):
        assert isinstance(node, nodes.FuncCall)
        return isinstance(node.lod, nodes.DefaultAggregationLodSpecifier)

    return False


def qfork_is_aggregation(node: fork_nodes.QueryFork) -> bool:
    return (
        not isinstance(node.lod, nodes.InheritedLodSpecifier)  # Only lookups have these
        and is_aggregate_function(node.result_expr)
    )


def qfork_is_window(node: fork_nodes.QueryFork) -> bool:
    return (
        not isinstance(node.lod, nodes.InheritedLodSpecifier)  # Only lookups have these
        and is_window_function(node.result_expr)
    )


def qfork_is_lookup(node: fork_nodes.QueryFork) -> bool:
    return isinstance(node.lod, nodes.InheritedLodSpecifier)  # Only lookups have these
