from __future__ import annotations

from itertools import chain
import logging

import dl_formula.core.nodes as nodes
from dl_formula.mutation.mutation import FormulaMutation


LOGGER = logging.getLogger(__name__)


class IgnoreParenthesisWrapperMutation(FormulaMutation):
    """
    A mutation that ignores (removes) all parenthesis wrappers
    because they do not change the meaning of any of the expressions,
    but may prevent comparisons and recognition of dimensions
    from working correctly.
    """

    def match_node(
        self,
        node: nodes.FormulaItem,
        parent_stack: tuple[nodes.FormulaItem, ...],
    ) -> bool:
        return type(node) is nodes.ParenthesizedExpr and node.level_tag is None

    def make_replacement(
        self,
        old: nodes.FormulaItem,
        parent_stack: tuple[nodes.FormulaItem, ...],
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.ParenthesizedExpr)
        return old.expr


class ConvertBlocksToFunctionsMutation(FormulaMutation):
    """
    A mutation that converts IF- and CASE-blocks to corresponding functions.
    """

    def match_node(
        self,
        node: nodes.FormulaItem,
        parent_stack: tuple[nodes.FormulaItem, ...],
    ) -> bool:
        return isinstance(node, nodes.IfBlock) or isinstance(node, nodes.CaseBlock)

    def _make_replacement_for_if_block(self, node: nodes.IfBlock) -> nodes.FuncCall:
        args: list[nodes.FormulaItem] = [
            *chain.from_iterable(if_part.children for if_part in node.if_list),
            node.else_expr,
        ]
        return nodes.FuncCall.make("if", args=args, meta=node.meta)

    def _make_replacement_for_case_block(self, node: nodes.CaseBlock) -> nodes.FuncCall:
        args: list[nodes.FormulaItem] = [
            node.case_expr,
            *chain.from_iterable(when_part.children for when_part in node.when_list),
        ]
        if node.else_expr is not None:
            args.append(node.else_expr)
        return nodes.FuncCall.make("case", args=args, meta=node.meta)

    def make_replacement(
        self,
        old: nodes.FormulaItem,
        parent_stack: tuple[nodes.FormulaItem, ...],
    ) -> nodes.FormulaItem:
        if isinstance(old, nodes.IfBlock):
            return self._make_replacement_for_if_block(old)
        elif isinstance(old, nodes.CaseBlock):
            return self._make_replacement_for_case_block(old)

        raise TypeError(type(old))
