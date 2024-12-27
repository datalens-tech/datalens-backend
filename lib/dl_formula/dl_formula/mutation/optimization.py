import abc
from functools import cached_property
import operator
from typing import (
    Any,
    Callable,
    ClassVar,
    Optional,
)

from dl_formula.core.dialect import DialectCombo
import dl_formula.core.nodes as nodes
from dl_formula.mutation.mutation import FormulaMutation


class OptimizeConstOperatorMutation(FormulaMutation, abc.ABC):
    _opt_ops: ClassVar[dict[str, Callable[[Any, Any], Any]]]

    def _get_value(self, node: nodes.FormulaItem) -> Any:
        assert isinstance(node, nodes.Binary)
        left, right = node.left, node.right
        assert isinstance(left, nodes.BaseLiteral)
        assert isinstance(right, nodes.BaseLiteral)
        return self._opt_ops[node.name](left.value, right.value)


class OptimizeConstComparisonMutation(OptimizeConstOperatorMutation):
    """
    A mutation that evaluates comparison expressions for constants
    and replaces them with the in-Python evaluation result.
    """

    _opt_ops = {
        "==": operator.eq,
        "_==": operator.eq,
        "!=": operator.ne,
        "_!=": operator.ne,
    }

    def match_node(
        self,
        node: nodes.FormulaItem,
        parent_stack: tuple[nodes.FormulaItem, ...],
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
        parent_stack: tuple[nodes.FormulaItem, ...],
    ) -> nodes.FormulaItem:
        value = self._get_value(old)
        assert isinstance(value, bool)
        return nodes.LiteralBoolean.make(value, meta=old.meta)


class OptimizeConstMathOperatorMutation(OptimizeConstOperatorMutation):
    """
    A mutation that evaluates math operators for constants
    and replaces them with the in-Python evaluation result.
    """

    _opt_ops = {
        "+": operator.add,
        "-": operator.sub,
        "*": operator.mul,
        "/": operator.truediv,
    }

    def match_node(
        self,
        node: nodes.FormulaItem,
        parent_stack: tuple[nodes.FormulaItem, ...],
    ) -> bool:
        return (
            isinstance(node, nodes.Binary)
            # Operator is supported
            and node.name in self._opt_ops
            # Both operands are constant numbers
            and isinstance(node.left, (nodes.LiteralFloat, nodes.LiteralInteger))
            and isinstance(node.right, (nodes.LiteralFloat, nodes.LiteralInteger))
            # Avoid division by zero!
            and not (node.name == "/" and not node.right.value)
        )

    def make_replacement(
        self,
        old: nodes.FormulaItem,
        parent_stack: tuple[nodes.FormulaItem, ...],
    ) -> nodes.FormulaItem:
        value = self._get_value(old)
        assert isinstance(value, (int, float))
        node_cls = nodes.LiteralInteger if isinstance(value, int) else nodes.LiteralFloat
        return node_cls.make(value, meta=old.meta)


class OptimizeZeroOneComparisonMutation(FormulaMutation, abc.ABC):
    """
    Base class for optimizing ==/!= with 0/1.
    """

    _supported_operators: ClassVar[tuple[str, ...]]

    def match_node(self, node: nodes.FormulaItem, parent_stack: tuple[nodes.FormulaItem, ...]) -> bool:
        if not (isinstance(node, nodes.Binary) and node.name in ("==", "!=")):
            return False

        # should be either (binary op) ==/!= literal or literal ==/!= (binary op)
        left, right = node.left, node.right
        if isinstance(left, nodes.BaseLiteral) and isinstance(right, nodes.Binary):
            left, right = right, left
        if not (isinstance(left, nodes.Binary) and isinstance(right, nodes.BaseLiteral)):
            return False

        # both a binary op and a literal should be supported
        if not (left.name in self._supported_operators and right.value in (0, 1)):
            return False
        return True

    @abc.abstractmethod
    def _make_negative_replacement(self, opt: nodes.Binary) -> nodes.FormulaItem:
        """Replacement for == 0 and != 1"""
        raise NotImplementedError

    def make_replacement(
        self, old: nodes.FormulaItem, parent_stack: tuple[nodes.FormulaItem, ...]
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.Binary)
        left, right = old.left, old.right
        if isinstance(left, nodes.BaseLiteral) and isinstance(right, nodes.Binary):
            left, right = right, left
        assert isinstance(left, nodes.Binary) and isinstance(right, nodes.BaseLiteral)

        opt, lit = left, right  # aliases for convenience
        if (old.name == "==" and lit.value == 1) or (old.name == "!=" and lit.value == 0):
            return opt
        if (old.name == "==" and lit.value == 0) or (old.name == "!=" and lit.value == 1):
            return self._make_negative_replacement(opt)
        raise ValueError(f"Unexpected binary operation {old.name} and/or literal {lit.value} in optimization")


class OptimizeBinaryOperatorComparisonMutation(OptimizeZeroOneComparisonMutation):
    """
    A mutation that optimizes comparison with a result of another binary comparison.
    Originally intended to be used only for the filters section of a query, for example:
    - WHERE (A < B) == 1 -> WHERE A < B. Also works for "== true".
    - WHERE (A != B) == 0 -> WHERE A == B. Also works for "== false".
    """

    _opt_inversions = {
        ">": "<=",
        ">=": "<",
        "<": ">=",
        "<=": ">",
        "==": "!=",
        "!=": "==",
        "in": "notin",
        "notin": "in",
    }
    _supported_operators = tuple(_opt_inversions.keys())

    def _make_negative_replacement(self, opt: nodes.Binary) -> nodes.FormulaItem:
        return nodes.Binary.make(
            name=self._opt_inversions[opt.name],
            left=opt.left,
            right=opt.right,
            meta=opt.meta,
        )


class OptimizeAndOrComparisonMutation(OptimizeZeroOneComparisonMutation):
    """
    A mutation that optimizes comparison with a result of AND/OR operators.
    Originally intended to be used only for the filters section of a query, for example:
    - WHERE (A < B OR C < D) == 1 -> WHERE A < B OR C < D. Also works for "== true".
    - WHERE (A < B OR C < D) == 0 -> WHERE NOT (A < B OR C < D). Also works for "== false".
    """

    _supported_operators = ("and", "or")

    def _make_negative_replacement(self, opt: nodes.Binary) -> nodes.FormulaItem:
        return nodes.Unary.make(name="not", expr=opt)


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

    def match_node(self, node: nodes.FormulaItem, parent_stack: tuple[nodes.FormulaItem, ...]) -> bool:
        return (
            isinstance(node, nodes.OperationCall)
            and len(node.args) == 1
            and node.name in self.opt_ops
            and isinstance(node.args[0], nodes.BaseLiteral)
        )

    def make_replacement(
        self, old: nodes.FormulaItem, parent_stack: tuple[nodes.FormulaItem, ...]
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
        parent_stack: tuple[nodes.FormulaItem, ...],
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
        parent_stack: tuple[nodes.FormulaItem, ...],
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
        def optimize(self, node: nodes.FuncCall) -> nodes.FormulaItem:
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

        def optimize(self, node: nodes.FuncCall) -> nodes.FormulaItem:
            new_args: list[nodes.FormulaItem] = []
            for cond_arg, then_arg in zip(node.args[:-1:2], node.args[1:-1:2], strict=True):
                if isinstance(cond_arg, nodes.BaseLiteral):
                    if cond_arg.value is False:
                        # The condition is false, so skip this branch (don't add it to the optimized IF)
                        pass
                    if cond_arg.value is True:
                        # This is the first true condition, so the following branches are obsolete;
                        # add its "then" part as an "else" branch of the optimized IF and stop
                        new_args.append(then_arg)
                        break

                else:
                    # Add the args without changes
                    new_args.append(cond_arg)
                    new_args.append(then_arg)
            else:  # if no break occurred, we haven't optimized the "else" branch out, add it back
                new_args.append(node.args[-1])

            if len(new_args) == len(node.args):
                # Nothing seems to have changed
                return node

            if len(new_args) == 1:
                # precisely one condition left ("else" or a single "then"), return it as is
                return new_args[-1]

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

        def optimize(self, node: nodes.FuncCall) -> nodes.FormulaItem:
            case_expr_node = node.args[0]
            if not isinstance(case_expr_node, nodes.BaseLiteral):
                return node

            new_args: list[nodes.FormulaItem] = []
            new_args.append(case_expr_node)
            case_expr_value = case_expr_node.value
            for when_arg, then_arg in zip(node.args[1:-1:2], node.args[2:-1:2], strict=True):
                if isinstance(when_arg, nodes.BaseLiteral):
                    when_expr_value = when_arg.value
                    if when_expr_value != case_expr_value:
                        # The "when" is not the same as "case", so skip this branch
                        # (don't add it to the optimized CASE)
                        pass
                    else:  # They are equal
                        # This is the first matching "when", so the following branches are obsolete;
                        # add its "then" part as an "else" branch of the optimized CASE and stop
                        new_args.append(then_arg)
                        break

                else:
                    # Add the args without changes
                    new_args.append(when_arg)
                    new_args.append(then_arg)
            else:  # if no break occurred, we haven't optimized the "else" branch out, add it back
                new_args.append(node.args[-1])

            if len(new_args) == len(node.args):
                # Nothing seems to have changed
                return node

            if len(new_args) == 2:
                # precisely one condition left ("else" or a single "then"), return it as is
                return new_args[-1]

            return nodes.FuncCall.make("case", args=new_args, meta=node.meta)

    _func_optimizers: dict[str, ConstFuncOptimizer] = {
        "if": IfFuncOptimizer(),
        "case": CaseFuncOptimizer(),
    }

    def match_node(
        self,
        node: nodes.FormulaItem,
        parent_stack: tuple[nodes.FormulaItem, ...],
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
        parent_stack: tuple[nodes.FormulaItem, ...],
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.FuncCall)

        return self._func_optimizers[old.name].optimize(old)
