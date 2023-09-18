from __future__ import annotations

import logging
from typing import (
    Collection,
    Dict,
    FrozenSet,
    Optional,
    Tuple,
)

import dl_formula.core.fork_nodes as fork_nodes
import dl_formula.core.nodes as nodes
from dl_formula.inspect.function import supports_bfb
from dl_formula.mutation.mutation import FormulaMutation

LOGGER = logging.getLogger(__name__)


class NormalizeBeforeFilterByMutation(FormulaMutation):
    """
    Normalizes ``BeforeFilterBy`` clauses of functions that support it
    based on these clauses of parent functions.

    Example:
        bfb - BEFORE FILTER BY
        Top - highest level function call; bottom - the most nested function call

        Available filters: A, C, D

        Original
        func call
        hierarchy:                  Mutated:

            F bfb()                    F bfb()
            F bfb(A)                   F bfb(A)
            F bfb()                    F bfb(A)
            F bfb(A,B)         -->     F bfb(A)
            F bfb()                    F bfb(A)
            F bfb(A,B,C,D)             F bfb(A,C,D)   # B is ignored here because it isn't available
            F bfb()                    F bfb(A,C,D)
    """

    def __init__(self, available_filter_field_ids: Collection[str]):
        self._available_filter_field_ids = frozenset(available_filter_field_ids)

    def match_node(self, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]) -> bool:
        if not isinstance(node, nodes.BeforeFilterBy):
            return False

        # One exception to the rule: do not normalize BFBs for functions that do not support BFBs.
        # These BFBs should remain empty
        parent = parent_stack[-1]
        if isinstance(parent, nodes.FuncCall):
            is_window = isinstance(parent, nodes.WindowFuncCall)
            if not supports_bfb(parent.name, is_window=is_window):
                return False

        return True

    def make_replacement(
        self, old: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.BeforeFilterBy)
        assert parent_stack
        func = parent_stack[-1]
        # By this point fork functions may be transformed into QueryFork,
        # which inherit the original functions' BFB clauses,
        # so we have to allow for that possibility here
        assert isinstance(func, (nodes.FuncCall, fork_nodes.QueryFork))

        parent_bfb_names: FrozenSet[str] = frozenset()
        for parent in reversed(parent_stack):
            if isinstance(parent, (nodes.FuncCall, fork_nodes.QueryFork)):
                parent_bfb_names |= parent.before_filter_by.field_names

        cur_bfb_names = (old.field_names | parent_bfb_names) & self._available_filter_field_ids
        if isinstance(func, nodes.FuncCall):
            name_str = f"function {func.name}"
        else:
            name_str = "query fork"

        if cur_bfb_names:
            LOGGER.info("Normalizing BEFORE FILTER BY names %s to %s for %s", old.field_names, cur_bfb_names, name_str)

        return nodes.BeforeFilterBy.make(field_names=cur_bfb_names, meta=old.meta)


class RemapBfbMutation(FormulaMutation):
    """
    Applies a name mapper to all BEFORE FILTER BY clauses.
    """

    def __init__(self, name_mapping: Optional[Dict[str, str]] = None):
        self._name_mapping = name_mapping

    def remap_name(self, name: str) -> str:
        if self._name_mapping is not None:
            name = self._name_mapping.get(name, name)
        return name

    def match_node(self, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]) -> bool:
        return isinstance(node, nodes.BeforeFilterBy)

    def make_replacement(
        self, old: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.BeforeFilterBy)

        remapped_names = frozenset({self.remap_name(name) for name in old.field_names})
        return nodes.BeforeFilterBy.make(field_names=remapped_names, meta=old.meta)
