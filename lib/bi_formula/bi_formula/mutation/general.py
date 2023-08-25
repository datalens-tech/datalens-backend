from __future__ import annotations

import logging
from functools import cached_property
from itertools import chain
from typing import Any, Callable, Optional, Tuple

import bi_formula.core.nodes as nodes
from bi_formula.core.dialect import DialectCombo
from bi_formula.mutation.mutation import FormulaMutation


LOGGER = logging.getLogger(__name__)


class IgnoreParenthesisWrapperMutation(FormulaMutation):
    """
    A mutation that ignores (removes) all parenthesis wrappers
    because they do not change the meaning of any of the expressions,
    but may prevent comparisons and recognition of dimensions
    from working correctly.
    """

    def match_node(
            self, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...],
    ) -> bool:
        return type(node) is nodes.ParenthesizedExpr and node.level_tag is None

    def make_replacement(
            self, old: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...],
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.ParenthesizedExpr)
        return old.expr


class ConvertBlocksToFunctionsMutation(FormulaMutation):
    """
    A mutation that converts IF- and CASE-blocks to corresponding functions.
    """

    def match_node(
            self, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...],
    ) -> bool:
        return isinstance(node, nodes.IfBlock) or isinstance(node, nodes.IfBlock)

    def _make_replacement_for_if_block(self, node: nodes.IfBlock) -> nodes.FuncCall:
        args: list[nodes.FormulaItem] = [
            *chain.from_iterable(if_part.children for if_part in node.if_list),  # type: ignore
            node.else_expr
        ]
        return nodes.FuncCall.make('if', args=args, meta=node.meta)

    def _make_replacement_for_case_block(self, node: nodes.CaseBlock) -> nodes.FuncCall:
        args: list[nodes.FormulaItem] = [
            node.case_expr,
            *chain.from_iterable(when_part.children for when_part in node.when_list)  # type: ignore
        ]
        if node.else_expr is not None:
            args.append(node.else_expr)
        return nodes.FuncCall.make('case', args=args, meta=node.meta)

    def make_replacement(
            self, old: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...],
    ) -> nodes.FormulaItem:
        if isinstance(old, nodes.IfBlock):
            return self._make_replacement_for_if_block(old)
        elif isinstance(old, nodes.CaseBlock):
            return self._make_replacement_for_case_block(old)

        raise TypeError(type(old))


class OptimizeConstComparisonMutation(FormulaMutation):
    """
    A mutation that evaluates comparison expressions for constants
    and replaces them with the in-Python evaluation result.
    """

    _opt_ops = {
        '==': lambda left, right: left == right,
        '_==': lambda left, right: left == right,
        '!=': lambda left, right: left != right,
        '_!=': lambda left, right: left != right,
    }

    def match_node(
            self, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...],
    ) -> bool:
        return (
            isinstance(node, nodes.Binary)
            # Operator is supported
            and node.name in self._opt_ops
            # Both operands are constants
            and isinstance(node.left, nodes.BaseLiteral)
            and isinstance(node.right, nodes.BaseLiteral)
            # To stay on the safe side, only do this for same-type constants
            and type(node.left) is type(node.right)  # noqa
        )

    def make_replacement(
            self, old: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...],
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.Binary)
        left, right = old.left, old.right
        assert isinstance(left, nodes.BaseLiteral)
        assert isinstance(right, nodes.BaseLiteral)
        value = self._opt_ops[old.name](left.value, right.value)
        return nodes.LiteralBoolean.make(value, meta=old.meta)


class OptimizeUnaryBoolFunctions(FormulaMutation):
    _opt_ops: dict[str, dict[Optional[DialectCombo], Callable[[Any], bool]]] = {
        'isnull': {None: lambda node: node is None},
    }

    def __init__(self, dialects: DialectCombo):
        self.dialects = dialects

    @cached_property
    def opt_ops(self) -> dict[str, Callable[[Any], bool]]:
        lst = self.dialects.to_list()
        opt_ops = {}
        for k, v in self._opt_ops.items():
            if len(v) == 1:
                opt_ops[k] = v[None]
            elif self.dialects in v:
                opt_ops[k] = v[self.dialects]
            else:
                for dialect in lst:
                    if dialect in v:
                        opt_ops[k] = v[self.dialects]
                        break
                else:
                    opt_ops[k] = v[None]
        return opt_ops

    def match_node(
            self, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]
    ) -> bool:
        return (
            isinstance(node, nodes.OperationCall)
            and len(node.args) == 1
            and node.name in self.opt_ops
            and isinstance(node.args[0], nodes.BaseLiteral)
        )

    def make_replacement(
            self, old: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.OperationCall)
        f = self.opt_ops[old.name]
        const_node = old.args[0]
        assert isinstance(const_node, nodes.BaseLiteral)
        return nodes.LiteralBoolean.make(value=f(const_node.value), meta=old.meta)

    @classmethod
    def register_dialect(cls, func_name: str, dialects: DialectCombo, f: Callable[[Any], bool]) -> None:
        for dialect in dialects.to_list(with_self=True):
            cls._opt_ops[func_name][dialect] = f


class OptimizeConstAndOrMutation(FormulaMutation):
    """
    A mutation that simplifies expressions such as:
    - <expr> AND <const>
    - <expr> OR <const>
    """

    _opt_ops = {
        'and': {
            True: lambda node, meta: node,
            False: lambda node, meta: nodes.LiteralBoolean.make(value=False, meta=meta),
        },
        'or': {
            True: lambda node, meta: nodes.LiteralBoolean.make(value=True, meta=meta),
            False: lambda node, meta: node,
        },
    }

    def match_node(
            self, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...],
    ) -> bool:
        return (
            isinstance(node, nodes.Binary)
            # Operator is supported
            and node.name in self._opt_ops
            # One of the operands is a constant
            and (isinstance(node.left, nodes.BaseLiteral) or isinstance(node.right, nodes.BaseLiteral))
        )

    def make_replacement(
            self, old: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...],
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.Binary)

        const_node: nodes.BaseLiteral
        expr_node: nodes.FormulaItem
        if isinstance(old.left, nodes.BaseLiteral):
            const_node, expr_node = old.left, old.right
        else:
            assert isinstance(old.right, nodes.BaseLiteral)
            const_node, expr_node = old.right, old.left

        new_node = self._opt_ops[old.name][bool(const_node.value)](expr_node, old.meta)
        return new_node
