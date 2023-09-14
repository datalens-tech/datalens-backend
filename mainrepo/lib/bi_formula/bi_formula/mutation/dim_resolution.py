from __future__ import annotations

from itertools import chain
from typing import (
    TYPE_CHECKING,
    List,
    Tuple,
)

import attr

import bi_formula.core.nodes as nodes
from bi_formula.inspect.env import InspectionEnvironment
from bi_formula.inspect.expression import resolve_dimensions
from bi_formula.mutation.mutation import FormulaMutation

if TYPE_CHECKING:
    from bi_formula.collections import NodeSet


@attr.s
class DimensionResolvingMutationBase(FormulaMutation):
    """
    A mutation base class that can resolve the expression's dimensions at any depth.
    """

    _global_dimensions: List[nodes.FormulaItem] = attr.ib(kw_only=True)
    _inspect_env: InspectionEnvironment = attr.ib(kw_only=True, factory=InspectionEnvironment)

    def _generate_dimensions(
        self, node: nodes.FormulaItem, parent_stack: Tuple[nodes.FormulaItem, ...]
    ) -> Tuple[List[nodes.FormulaItem], NodeSet, NodeSet]:
        """
        Return dimension list for ``old`` and dimensions of the parent node
        ("the surrounding environment")
        """

        dimensions, dimension_set, parent_dimension_set = resolve_dimensions(
            node_stack=chain(parent_stack, (node,)),
            dimensions=self._global_dimensions,
            env=self._inspect_env,
        )
        return dimensions, dimension_set, parent_dimension_set
