from __future__ import annotations

from typing import Tuple

import bi_formula.core.nodes as nodes
from bi_formula.mutation.mutation import FormulaMutation
from bi_formula.mutation.mutation import apply_mutations
from bi_formula.shortcuts import n


class PrefixFunctionMutation(FormulaMutation):
    """Prepend `s` to function names"""

    def __init__(self, prefix: str):
        self._prefix = prefix

    def match_node(
            self, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]
    ) -> bool:
        return isinstance(node, nodes.FuncCall)

    def make_replacement(
            self, old: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.FuncCall)
        return nodes.FuncCall.make(name=self._prefix + old.name, args=old.args)


class PostfixFieldMutation(FormulaMutation):
    """Append `s` to field names"""

    def __init__(self, postfix: str):
        self._postfix = postfix

    def match_node(
            self, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]
    ) -> bool:
        return isinstance(node, nodes.Field)

    def make_replacement(
            self, old: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.Field)
        return nodes.Field.make(name=old.name + self._postfix)


def test_multiple_mutations():
    assert apply_mutations(
        n.formula(n.func.MYFUNC(n.field('field'))),
        mutations=[
            # note that they are applied in the same order as they are listed,
            # so w need to reverse them here to get a sensible result
            PrefixFunctionMutation('c'),
            PrefixFunctionMutation('b'),
            PrefixFunctionMutation('a'),
            PostfixFieldMutation('1'),
            PostfixFieldMutation('2'),
        ]
    ) == n.formula(n.func.ABCMYFUNC(n.field('field12')))
