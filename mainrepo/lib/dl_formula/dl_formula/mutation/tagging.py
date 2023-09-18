from __future__ import annotations

from enum import Enum
from typing import Tuple

import dl_formula.core.fork_nodes as fork_nodes
import dl_formula.core.nodes as nodes
from dl_formula.core.tag import LevelTag
from dl_formula.inspect.expression import (
    get_qfork_wrapping_level_until_winfunc,
    get_window_function_wrapping_level,
)
from dl_formula.inspect.function import supports_bfb
from dl_formula.inspect.node import is_aggregate_function
from dl_formula.mutation.mutation import FormulaMutation


class LevelTagType(Enum):
    win_func = "win_func"
    query_fork = "query_fork"


class FunctionLevelTagMutation(FormulaMutation):
    """
    Tags all functions based on their ``BeforeFilterBy`` clause
    and nesting level among functions with the same ``BeforeFilterBy``.

        Original
        hierarchy:                       Mutated:

            F bfb()                    + tag ((), 0)
            F bfb(A)                   + tag ((A,), 0)
            F bfb(A)                   + tag ((A,), 1)
            F bfb(A)                   + tag ((A,), 2)
            F bfb(A,B)         -->     + tag ((A,B), 0)
            F bfb(A,B)                 + tag ((A,B), 1)
            F bfb(A,B,C,D)             + tag ((A,B,C,D), 0)
    """

    @staticmethod
    def _node_is_taggable(node: nodes.FormulaItem) -> bool:
        if isinstance(node, nodes.FuncCall):
            is_window = isinstance(node, nodes.WindowFuncCall)
            return (
                supports_bfb(name=node.name, is_window=is_window)
                # Omit regular aggregations until full implementation of LODs
                and not is_aggregate_function(node)
            )

        return isinstance(node, fork_nodes.QueryFork)

    @staticmethod
    def _node_is_already_tagged(node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]) -> bool:
        if isinstance(node, nodes.FuncCall):
            assert parent_stack
            parent = parent_stack[-1]
            return isinstance(parent, nodes.ParenthesizedExpr) and parent.level_tag is not None

        if isinstance(node, fork_nodes.QueryFork):
            return node.level_tag is not None

        raise TypeError(type(node))

    @staticmethod
    def _get_tag_level_type_for_node(node: nodes.FormulaItem) -> LevelTagType:
        if isinstance(node, nodes.FuncCall):
            return LevelTagType.win_func

        if isinstance(node, fork_nodes.QueryFork):
            return LevelTagType.query_fork

        raise TypeError(type(node))

    @staticmethod
    def _make_tagged_node(node: nodes.FormulaItem, level_tag: LevelTag) -> nodes.FormulaItem:
        if isinstance(node, nodes.WindowFuncCall):
            return nodes.ParenthesizedExpr.make(expr=node, meta=node.meta.with_tag(level_tag))
        if isinstance(node, fork_nodes.QueryFork):
            return node.with_tag(level_tag=level_tag)
        raise TypeError(type(node))

    @classmethod
    def match_node(cls, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]) -> bool:
        return cls._node_is_taggable(node) and not cls._node_is_already_tagged(node, parent_stack)

    def make_replacement(
        self, old: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]
    ) -> nodes.FormulaItem:
        assert isinstance(old, (nodes.FuncCall, fork_nodes.QueryFork))
        cur_field_names = old.before_filter_by.field_names

        # We cannot correctly calculate nesting level bottom-up
        # by inspecting the parent stack
        # because some of the nested functions already have
        # this mutation applied to them and will be skipped in this round.
        # This can happen if they were substituted here as part of a different field.
        # Because of all this to preserve the correct order of BFB tags
        # we need to calculate nesting_level as the negative wrapping level
        # (going top-down by recursively scanning nested functions with the same BFB names)

        bfb_names = old.before_filter_by.field_names
        wfunc_wrap_level = get_window_function_wrapping_level(old, bfb_names=bfb_names)
        qfork_wrap_level = get_qfork_wrapping_level_until_winfunc(old, bfb_names=bfb_names)
        # Invert them so that the outermost nodes have lower (negative) nesting levels
        final_func_nesting_level, final_qfork_nesting_level = -wfunc_wrap_level, -qfork_wrap_level

        level_tag = LevelTag(
            bfb_names=cur_field_names,
            func_nesting=final_func_nesting_level,
            qfork_nesting=final_qfork_nesting_level,
        )
        return self._make_tagged_node(old, level_tag=level_tag)
