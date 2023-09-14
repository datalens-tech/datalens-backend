"""
Auxiliary nodes specific to the forked subquery joining mechanism.
"""

from __future__ import annotations

from enum import Enum
from typing import (
    Hashable,
    Optional,
    Sequence,
    cast,
)

import bi_formula.core.nodes as nodes


class JoinType(Enum):
    inner = "inner"
    left = "left"


class JoinConditionBase(nodes.FormulaItem):
    """Base class for join conditions"""

    __slots__ = ()
    autonomous = False

    @classmethod
    def validate_internal_value(cls, internal_value: tuple[Optional[Hashable], ...]) -> None:
        assert not internal_value


class SelfEqualityJoinCondition(JoinConditionBase):
    """
    A join condition that indicates that the values
    of the same dimension equal each other in main subquery and its fork.
    """

    __slots__ = ()
    show_names = JoinConditionBase.show_names + ("expr",)

    expr: nodes.Child[nodes.FormulaItem] = nodes.Child(0)

    @classmethod
    def validate_children(cls, children: Sequence[nodes.FormulaItem]) -> None:
        assert len(children) == 1

    @classmethod
    def make(cls, expr: nodes.FormulaItem, *, meta: Optional[nodes.NodeMeta] = None) -> SelfEqualityJoinCondition:
        children = (expr,)
        return cls(*children, meta=meta)


class BinaryJoinCondition(JoinConditionBase):
    """
    A join condition that indicates that
    ``expr`` from the main subquery equals ``fork_expr`` from the fork subquery.
    """

    __slots__ = ()
    show_names = JoinConditionBase.show_names + ("expr", "fork_expr")

    expr: nodes.Child[nodes.FormulaItem] = nodes.Child(0)
    fork_expr: nodes.Child[nodes.FormulaItem] = nodes.Child(1)

    @classmethod
    def validate_children(cls, children: Sequence[nodes.FormulaItem]) -> None:
        assert len(children) == 2

    @classmethod
    def make(
        cls,
        expr: nodes.FormulaItem,
        fork_expr: nodes.FormulaItem,
        *,
        meta: Optional[nodes.NodeMeta] = None,
    ) -> BinaryJoinCondition:
        children = (expr, fork_expr)
        return cls(*children, meta=meta)


class QueryForkJoiningBase(nodes.FormulaItem):
    __slots__ = ()
    autonomous = False

    @property
    def is_self_eq_join(self) -> bool:
        raise NotImplementedError


class QueryForkJoiningWithList(QueryForkJoiningBase):
    """
    Represents a join using all dimensions with custom condition overrides
    """

    __slots__ = ()

    show_names = QueryForkJoiningBase.show_names + ("condition_list",)

    condition_list: nodes.MultiChild[JoinConditionBase] = nodes.MultiChild(slice(0, None))

    @classmethod
    def make(
        cls,
        condition_list: list[JoinConditionBase],
        *,
        meta: Optional[nodes.NodeMeta] = None,
    ) -> QueryForkJoiningWithList:
        children = condition_list
        return cls(*children, meta=meta)

    @classmethod
    def validate_internal_value(cls, internal_value: tuple[Optional[Hashable], ...]) -> None:
        assert not internal_value

    @property
    def is_self_eq_join(self) -> bool:
        """All join conditions are ``SelfEqualityJoinCondition``"""
        return all(isinstance(child, SelfEqualityJoinCondition) for child in self.children)


class QueryFork(nodes.FormulaItem):
    """
    Represents a point where the query should be forked in two:
    the main subquery and the forked subquery.
    The ``joining`` child node describes how the two subqueries should be joined
    """

    __slots__ = ()

    show_names = nodes.FormulaItem.show_names + ("join_type", "joining", "result_expr", "lod")

    joining: nodes.Child[QueryForkJoiningBase] = nodes.Child(0)
    result_expr: nodes.Child[nodes.FormulaItem] = nodes.Child(1)
    before_filter_by: nodes.Child[nodes.BeforeFilterBy] = nodes.Child(2)
    lod: nodes.Child[nodes.LodSpecifier] = nodes.Child(3)

    @classmethod
    def make(
        cls,
        join_type: JoinType,
        joining: QueryForkJoiningBase,
        result_expr: nodes.FormulaItem,
        before_filter_by: Optional[nodes.BeforeFilterBy] = None,
        lod: Optional[nodes.LodSpecifier] = None,
        meta: Optional[nodes.NodeMeta] = None,
    ) -> QueryFork:
        if before_filter_by is None:
            before_filter_by = nodes.BeforeFilterBy.make()
        if lod is None:
            lod = nodes.InheritedLodSpecifier()

        children = (joining, result_expr, before_filter_by, lod)
        internal_value = (join_type,)
        return cls(*children, internal_value=internal_value, meta=meta)

    @classmethod
    def validate_children(cls, children: Sequence[nodes.FormulaItem]) -> None:
        assert len(children) == 4

    @classmethod
    def validate_internal_value(cls, internal_value: tuple[Optional[Hashable], ...]) -> None:
        assert len(internal_value) == 1
        assert isinstance(internal_value[0], JoinType)

    @property
    def join_type(self) -> JoinType:
        return cast(JoinType, self.internal_value[0])
