"""
Mutations allow making modification of nodes of a formula expression
that meet certain criteria.

Can be used to prepare the formula for translation and also for optimization of certain node constructs
"""

from __future__ import annotations

import abc
from typing import (
    Sequence,
    Tuple,
    TypeVar,
)

import dl_formula.core.nodes as nodes


class FormulaMutation(abc.ABC):
    """Class described how a formula object is transformed via replacements of its nodes"""

    @abc.abstractmethod
    def match_node(self, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]) -> bool:
        """Check whether the given node matches internal criteria for replacement"""

        raise NotImplementedError

    @abc.abstractmethod
    def make_replacement(
        self, old: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]
    ) -> nodes.FormulaItem:
        """Generate a new node that will replace the old one in the formula"""

        raise NotImplementedError


_NODE_TV = TypeVar("_NODE_TV", bound=nodes.FormulaItem)


def apply_mutations(tree: _NODE_TV, mutations: Sequence[FormulaMutation]) -> _NODE_TV:
    """Apply multiple mutations to formula node tree"""

    def match_func(node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]) -> bool:
        for mutation in mutations:
            if mutation.match_node(node, parent_stack=parent_stack):
                return True
        return False

    def replace_func(node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]) -> nodes.FormulaItem:
        replacement_made = False
        for mutation in mutations:
            if mutation.match_node(node, parent_stack=parent_stack):
                replacement_made = True
                node = mutation.make_replacement(node, parent_stack=parent_stack)
        if replacement_made:
            return node
        raise RuntimeError("Could not perform node replacement")

    return tree.replace_nodes(match_func=match_func, replace_func=replace_func)
