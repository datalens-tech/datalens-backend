from __future__ import annotations

import abc
from enum import Enum
from typing import (
    Callable,
    Optional,
)
import uuid

import attr

import dl_formula.core.fork_nodes as fork_nodes
import dl_formula.core.nodes as nodes
from dl_formula.core.tag import LevelTag
from dl_formula.inspect.env import InspectionEnvironment
import dl_formula.inspect.expression
import dl_formula.inspect.function
import dl_formula.inspect.node


class BoundaryCheckResult(Enum):
    raise_level = "raise_level"
    neutral = "neutral"
    maintain_level = "maintain_level"


@attr.s
class LevelBoundary(abc.ABC):
    name: str = attr.ib()
    name_gen: Optional[Callable[[nodes.FormulaItem, str], str]] = attr.ib(default=None)

    def _default_name_gen(self, node: nodes.FormulaItem) -> str:
        """Default name generator for sliced nodes"""
        return str(uuid.uuid4())

    def generate_sliced_node_name(self, node: nodes.FormulaItem) -> str:
        if self.name_gen is not None:
            return self.name_gen(node, self.name)
        return self._default_name_gen(node)

    @abc.abstractmethod
    def check_node(
        self,
        node: nodes.FormulaItem,
        inspect_env: InspectionEnvironment,
        parent_stack: tuple[nodes.FormulaItem, ...],
    ) -> BoundaryCheckResult:
        raise NotImplementedError

    def should_raise_level(
        self,
        node: nodes.FormulaItem,
        inspect_env: InspectionEnvironment,
        parent_stack: tuple[nodes.FormulaItem, ...],
    ) -> bool:
        """Return ``True`` if the expression should be raised to the next execution level"""
        return (
            self.check_node(
                node,
                inspect_env=inspect_env,
                parent_stack=parent_stack,
            )
            == BoundaryCheckResult.raise_level
        )

    def should_maintain_level(
        self,
        node: nodes.FormulaItem,
        inspect_env: InspectionEnvironment,
        parent_stack: tuple[nodes.FormulaItem, ...],
    ) -> bool:
        """Return ``True`` if the expression should stay at the current execution level"""
        return (
            self.check_node(
                node,
                inspect_env=inspect_env,
                parent_stack=parent_stack,
            )
            == BoundaryCheckResult.maintain_level
        )


@attr.s
class AggregateFunctionLevelBoundary(LevelBoundary):
    """Matches aggregate functions"""

    constants_neutral: bool = attr.ib(default=True)

    def check_node(
        self,
        node: nodes.FormulaItem,
        inspect_env: InspectionEnvironment,
        parent_stack: tuple[nodes.FormulaItem, ...],
    ) -> BoundaryCheckResult:
        if dl_formula.inspect.node.is_aggregate_function(node):
            return BoundaryCheckResult.raise_level

        if self.constants_neutral:
            if dl_formula.inspect.expression.is_constant_expression(node, env=inspect_env):
                return BoundaryCheckResult.neutral

        return BoundaryCheckResult.maintain_level


@attr.s
class WindowFunctionLevelBoundary(LevelBoundary):
    """Matches window functions"""

    constants_neutral: bool = attr.ib(default=True)

    def check_node(
        self,
        node: nodes.FormulaItem,
        inspect_env: InspectionEnvironment,
        parent_stack: tuple[nodes.FormulaItem, ...],
    ) -> BoundaryCheckResult:
        if isinstance(node, nodes.WindowFuncCall):
            return BoundaryCheckResult.raise_level

        if self.constants_neutral:
            if dl_formula.inspect.expression.is_constant_expression(node, env=inspect_env):
                return BoundaryCheckResult.neutral

        return BoundaryCheckResult.maintain_level


class TopLevelBoundary(LevelBoundary):
    """Never matches any node"""

    def check_node(
        self,
        node: nodes.FormulaItem,
        inspect_env: InspectionEnvironment,
        parent_stack: tuple[nodes.FormulaItem, ...],
    ) -> BoundaryCheckResult:
        return BoundaryCheckResult.maintain_level


class NonFieldsBoundary(LevelBoundary):
    """Raises all non-simple-field nodes to the next level"""

    def check_node(
        self,
        node: nodes.FormulaItem,
        inspect_env: InspectionEnvironment,
        parent_stack: tuple[nodes.FormulaItem, ...],
    ) -> BoundaryCheckResult:
        if isinstance(node, nodes.Field):
            return BoundaryCheckResult.maintain_level
        return BoundaryCheckResult.raise_level


@attr.s
class NestedLevelTaggedBoundary(LevelBoundary):
    """
    Operates over level tags.
    Tags, whose ``bfb_names`` set part is a subset of ``self.tag.bfb_names`` are raised.
    if the ``bfb_names`` sets are equal, then lower or equal values
    of ``tag.nesting`` result in raising
    The behavior for constants is inherited from ``TaggedBoundary``
    All other nodes maintain their level.

    Example:
        self.tag = ({A, B}, 1)

        Hierarchy:
            T({A}, 0)
            T({A}, 1)
            T({A, B}, 0)
            T({A, B}, 1)      <-- The part below this tag is sliced off
            T({A, B}, 2)
            T({A, B, C}, 0)
    """

    tag: Optional[LevelTag] = attr.ib(default=None)
    constants_neutral: bool = attr.ib(default=True)

    def check_tag(self, is_own_tag: bool, level_tag: LevelTag) -> BoundaryCheckResult:
        if self.tag is None or (level_tag < self.tag or is_own_tag and level_tag <= self.tag):
            # For non-own tags use strict <
            return BoundaryCheckResult.raise_level
        return BoundaryCheckResult.maintain_level

    def get_node_tag(
        self,
        node: nodes.FormulaItem,
        parent_stack: tuple[nodes.FormulaItem, ...],
    ) -> tuple[bool, Optional[LevelTag]]:
        # For QueryForks the node itself is tagged
        if isinstance(node, fork_nodes.QueryFork):
            return True, node.level_tag

        parent = parent_stack[-1]

        # For aggregations functions, the wrapping QueryFork contains the tag
        if (
            dl_formula.inspect.node.is_aggregate_function(node)
            and isinstance(parent, fork_nodes.QueryFork)
            and dl_formula.inspect.node.qfork_is_aggregation(parent)  # only LODs, avoid lookups here
        ):
            return False, parent.level_tag

        # For window functions, the wrapping parentheses contain the tag
        if (
            isinstance(node, nodes.WindowFuncCall)
            and dl_formula.inspect.function.supports_bfb(node.name, is_window=True)
            and isinstance(parent, nodes.ParenthesizedExpr)
        ):
            return False, parent.level_tag

        # Tagged parentheses go hand in hand with window functions
        if isinstance(node, nodes.ParenthesizedExpr):
            return True, node.level_tag

        return True, None

    def check_node(
        self,
        node: nodes.FormulaItem,
        inspect_env: InspectionEnvironment,
        parent_stack: tuple[nodes.FormulaItem, ...],
    ) -> BoundaryCheckResult:
        is_own_tag, level_tag = self.get_node_tag(node=node, parent_stack=parent_stack)
        if level_tag is not None:
            return self.check_tag(is_own_tag=is_own_tag, level_tag=level_tag)

        if self.constants_neutral:
            if dl_formula.inspect.expression.is_constant_expression(node, env=inspect_env):
                return BoundaryCheckResult.neutral

        return BoundaryCheckResult.maintain_level


@attr.s
class SliceSchema:
    levels: list[LevelBoundary] = attr.ib(factory=list)

    @property
    def max_level(self) -> int:
        return len(self.levels) - 1
