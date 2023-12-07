from __future__ import annotations

import abc
from functools import cached_property
from itertools import chain
import logging
from typing import (
    Any,
    Callable,
    Optional,
    Tuple,
)

from dl_formula.core.dialect import DialectCombo
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
        parent_stack: Tuple[nodes.FormulaItem, ...],
    ) -> bool:
        return type(node) is nodes.ParenthesizedExpr and node.level_tag is None

    def make_replacement(
        self,
        old: nodes.FormulaItem,
        parent_stack: Tuple[nodes.FormulaItem, ...],
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
        parent_stack: Tuple[nodes.FormulaItem, ...],
    ) -> bool:
        return isinstance(node, nodes.IfBlock) or isinstance(node, nodes.CaseBlock)

    def _make_replacement_for_if_block(self, node: nodes.IfBlock) -> nodes.FuncCall:
        args: list[nodes.FormulaItem] = [
            *chain.from_iterable(if_part.children for if_part in node.if_list),  # type: ignore
            node.else_expr,
        ]
        return nodes.FuncCall.make("if", args=args, meta=node.meta)

    def _make_replacement_for_case_block(self, node: nodes.CaseBlock) -> nodes.FuncCall:
        args: list[nodes.FormulaItem] = [
            node.case_expr,
            *chain.from_iterable(when_part.children for when_part in node.when_list),  # type: ignore
        ]
        if node.else_expr is not None:
            args.append(node.else_expr)
        return nodes.FuncCall.make("case", args=args, meta=node.meta)

    def make_replacement(
        self,
        old: nodes.FormulaItem,
        parent_stack: Tuple[nodes.FormulaItem, ...],
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
        "==": lambda left, right: left == right,
        "_==": lambda left, right: left == right,
        "!=": lambda left, right: left != right,
        "_!=": lambda left, right: left != right,
    }

    def match_node(
        self,
        node: nodes.FormulaItem,
        parent_stack: Tuple[nodes.FormulaItem, ...],
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
        self,
        old: nodes.FormulaItem,
        parent_stack: Tuple[nodes.FormulaItem, ...],
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.Binary)
        left, right = old.left, old.right
        assert isinstance(left, nodes.BaseLiteral)
        assert isinstance(right, nodes.BaseLiteral)
        value = self._opt_ops[old.name](left.value, right.value)
        return nodes.LiteralBoolean.make(value, meta=old.meta)


class OptimizeUnaryBoolFunctions(FormulaMutation):
    _opt_ops: dict[str, dict[Optional[DialectCombo], Callable[[Any], bool]]] = {
        "isnull": {None: lambda node: node is None},
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

    def match_node(self, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]) -> bool:
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
        "and": {
            True: lambda node, meta: node,
            False: lambda node, meta: nodes.LiteralBoolean.make(value=False, meta=meta),
        },
        "or": {
            True: lambda node, meta: nodes.LiteralBoolean.make(value=True, meta=meta),
            False: lambda node, meta: node,
        },
    }

    def match_node(
        self,
        node: nodes.FormulaItem,
        parent_stack: Tuple[nodes.FormulaItem, ...],
    ) -> bool:
        return (
            isinstance(node, nodes.Binary)
            # Operator is supported
            and node.name in self._opt_ops
            # One of the operands is a constant
            and (isinstance(node.left, nodes.BaseLiteral) or isinstance(node.right, nodes.BaseLiteral))
        )

    def make_replacement(
        self,
        old: nodes.FormulaItem,
        parent_stack: Tuple[nodes.FormulaItem, ...],
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


class OptimizeConstFuncMutation(FormulaMutation):
    """
    A mutation that simplifies expressions such as:
    - <expr> AND <const>
    - <expr> OR <const>
    """

    class ConstFuncOptimizer(abc.ABC):
        @abc.abstractmethod
        def can_optimize(self, node: nodes.FuncCall) -> bool:
            raise NotImplementedError()

        @abc.abstractmethod
        def optimize(self, node: nodes.FuncCall) -> nodes.FuncCall:
            raise NotImplementedError()

    class IfFuncOptimizer(ConstFuncOptimizer):
        def can_optimize(self, node: nodes.FuncCall) -> bool:
            assert node.name == "if"
            if len(node.args) % 2 == 0:
                return False  # incorrect formula, skip
            for cond_arg in node.args[:-1:2]:  # excluding the last one (the "else" part) with a step of 2
                if isinstance(cond_arg, nodes.BaseLiteral):
                    return True

            return False

        def optimize(self, node: nodes.FuncCall) -> nodes.FuncCall:
            new_args: list[nodes.FormulaItem] = []
            for cond_arg, then_arg in zip(node.args[:-1:2], node.args[1:-1:2]):
                if isinstance(cond_arg, nodes.BaseLiteral):
                    if cond_arg.value is False:
                        # The condition is false, so skip this branch (don't add it to the optimized IF)
                        pass
                    if cond_arg.value is True:
                        # This is the first true condition, so just return its "then"
                        return then_arg

                else:
                    # Add the args without changes
                    new_args.append(cond_arg)
                    new_args.append(then_arg)

            new_args.append(node.args[-1])  # the "else"

            if len(new_args) == len(node.args):
                # Nothing seems to have changed
                return node

            if len(new_args) == 1:  # All conditions turned out to be false (only "else" in new args)
                return node.args[-1]  # the "else"

            return nodes.FuncCall.make("if", args=new_args, meta=node.meta)

    class CaseFuncOptimizer(ConstFuncOptimizer):
        def can_optimize(self, node: nodes.FuncCall) -> bool:
            assert node.name == "case"
            if len(node.args) % 2 == 1 or len(node.args) < 2:
                return False  # incorrect formula, skip
            if isinstance(node.args[0], nodes.BaseLiteral):
                # the CASE expression needs to be a const for the optimization
                for when_arg in node.args[1:-1:2]:
                    # CASE(<case_expr_0>, <when_expr_1>, <then_expr_2>, ..., <else_expr>)
                    if isinstance(when_arg, nodes.BaseLiteral):
                        return True

            return False

        def optimize(self, node: nodes.FuncCall) -> nodes.FuncCall:
            case_expr_node = node.args[0]
            if not isinstance(case_expr_node, nodes.BaseLiteral):
                return node

            new_args: list[nodes.FormulaItem] = []
            new_args.append(case_expr_node)
            case_expr_value = case_expr_node.value
            for when_arg, then_arg in zip(node.args[1:-1:2], node.args[2:-1:2]):
                if isinstance(when_arg, nodes.BaseLiteral):
                    when_expr_value = when_arg.value
                    if when_expr_value != case_expr_value:
                        # The "when" is not the same aas "case", so skip this branch
                        # (don't add it to the optimized CASE)
                        pass
                    else:  # They are equal
                        # This is the first matching "when", so just return its "then"
                        return then_arg

                else:
                    # Add the args without changes
                    new_args.append(when_arg)
                    new_args.append(then_arg)

            new_args.append(node.args[-1])  # the "else"

            if len(new_args) == len(node.args):
                # Nothing seems to have changed
                return node

            if len(new_args) == 2:  # All conditions turned out to be false
                return node.args[-1]  # the "else"

            return nodes.FuncCall.make("case", args=new_args, meta=node.meta)

    _func_optimizers: dict[str, ConstFuncOptimizer] = {
        "if": IfFuncOptimizer(),
        "case": CaseFuncOptimizer(),
    }

    def match_node(
        self,
        node: nodes.FormulaItem,
        parent_stack: Tuple[nodes.FormulaItem, ...],
    ) -> bool:
        return (
            isinstance(node, nodes.FuncCall)
            # Operator is supported
            and node.name in self._func_optimizers
            # One of the operands is a constant
            and self._func_optimizers[node.name].can_optimize(node)
        )

    def make_replacement(
        self,
        old: nodes.FormulaItem,
        parent_stack: Tuple[nodes.FormulaItem, ...],
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.FuncCall)

        return self._func_optimizers[old.name].optimize(old)
