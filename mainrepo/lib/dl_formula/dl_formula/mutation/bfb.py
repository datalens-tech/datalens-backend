from __future__ import annotations

import logging
from typing import Optional

import dl_formula.core.nodes as nodes
from dl_formula.mutation.mutation import FormulaMutation


LOGGER = logging.getLogger(__name__)


class RemapBfbMutation(FormulaMutation):
    """
    Applies a name mapper to all BEFORE FILTER BY clauses.
    """

    def __init__(self, name_mapping: Optional[dict[str, str]] = None):
        self._name_mapping = name_mapping

    def remap_name(self, name: str) -> str:
        if self._name_mapping is not None:
            name = self._name_mapping.get(name, name)
        return name

    def match_node(self, node: nodes.FormulaItem, parent_stack: tuple[nodes.FormulaItem, ...]) -> bool:
        return isinstance(node, nodes.BeforeFilterBy)

    def make_replacement(
        self, old: nodes.FormulaItem, parent_stack: tuple[nodes.FormulaItem, ...]
    ) -> nodes.FormulaItem:
        assert isinstance(old, nodes.BeforeFilterBy)

        remapped_names = frozenset({self.remap_name(name) for name in old.field_names})
        return nodes.BeforeFilterBy.make(field_names=remapped_names, meta=old.meta)
