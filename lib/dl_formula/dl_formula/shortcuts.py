from __future__ import annotations

import builtins
import datetime as datetime_mod
from functools import wraps
from typing import (
    Any,
    FrozenSet,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)
import uuid as uuid_mod

from sqlalchemy.sql.elements import ClauseElement

from dl_formula.core import (
    fork_nodes,
    nodes,
)
from dl_formula.core.tag import LevelTag
from dl_formula.translation import ext_nodes
from dl_formula.translation.context import TranslationCtx


def _require_type(expected_type: Union[type, Tuple[Union[type, nodes.Array], ...], nodes.Array]):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
    def decorator(func):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
        @wraps(func)
        def wrapper(self, value):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
            nodes.check_type(value, expected_type=expected_type)
            return func(self, value)

        return wrapper

    return decorator


def _norm(value: Any) -> nodes.FormulaItem:
    if not isinstance(value, nodes.FormulaItem):
        value = n.lit(value)
    return value


class NodeShortcut:
    @_require_type(
        (
            builtins.str,
            builtins.int,
            builtins.float,
            builtins.bool,
            datetime_mod.date,
            datetime_mod.datetime,
            uuid_mod.UUID,
            nodes.Array(int),
            nodes.Array(float),
            nodes.Array(str),
            ClauseElement,
            TranslationCtx,
            type(None),
        )
    )
    def lit(self, value: Any) -> Union[nodes.BaseLiteral, nodes.Null]:
        if isinstance(value, str):
            return self.str(value)
        if isinstance(value, bool):
            return self.bool(value)
        if isinstance(value, int):
            return self.int(value)
        if isinstance(value, float):
            return self.float(value)
        if isinstance(value, datetime_mod.datetime):
            return self.genericdatetime(value)
        if isinstance(value, datetime_mod.date):
            return self.date(value)
        if isinstance(value, uuid_mod.UUID):
            return self.uuid(value)
        if nodes.does_type_match(value, nodes.Array(int)):
            return nodes.LiteralArrayInteger.make(value)
        if nodes.does_type_match(value, nodes.Array(float)):
            return nodes.LiteralArrayFloat.make(value)
        if nodes.does_type_match(value, nodes.Array(str)):
            return nodes.LiteralArrayString.make(value)
        if isinstance(value, (ClauseElement, TranslationCtx)):
            return ext_nodes.CompiledExpression.make(value)
        if value is None:
            return self.null()
        raise TypeError(type(value))

    @_require_type(builtins.int)
    def int(self, value: builtins.int) -> nodes.LiteralInteger:
        return nodes.LiteralInteger.make(value)

    @_require_type(builtins.float)
    def float(self, value: builtins.float) -> nodes.LiteralFloat:
        return nodes.LiteralFloat.make(value)

    @_require_type(builtins.bool)
    def bool(self, value: builtins.bool) -> nodes.LiteralBoolean:
        return nodes.LiteralBoolean.make(value)

    @_require_type(datetime_mod.datetime)
    def datetime(self, value: datetime_mod.datetime) -> nodes.LiteralDatetime:
        return nodes.LiteralDatetime.make(value)

    @_require_type(datetime_mod.datetime)
    def genericdatetime(self, value: datetime_mod.datetime) -> nodes.LiteralGenericDatetime:
        return nodes.LiteralGenericDatetime.make(value)

    @_require_type(datetime_mod.date)
    def date(self, value: datetime_mod.date) -> nodes.LiteralDate:
        return nodes.LiteralDate.make(value)

    @_require_type(builtins.str)
    def geopt(self, value: builtins.str) -> nodes.LiteralGeopoint:
        return nodes.LiteralGeopoint.make(value)

    @_require_type(builtins.str)
    def geopoly(self, value: builtins.str) -> nodes.LiteralGeopolygon:
        return nodes.LiteralGeopolygon.make(value)

    @_require_type((builtins.str, uuid_mod.UUID))
    def uuid(self, value: Union[builtins.str, uuid_mod.UUID]) -> nodes.LiteralUuid:
        return nodes.LiteralUuid.make(value)

    @_require_type(builtins.str)
    def str(self, value: builtins.str) -> nodes.LiteralString:
        return nodes.LiteralString.make(value)

    @_require_type((ClauseElement, TranslationCtx))
    def expr(self, value: Union[ClauseElement, TranslationCtx]) -> ext_nodes.CompiledExpression:
        return ext_nodes.CompiledExpression.make(value)

    def null(self) -> nodes.Null:
        return nodes.Null()

    def total(self) -> nodes.WindowGroupingTotal:
        return nodes.WindowGroupingTotal()

    def within(self, *dims: nodes.FormulaItem) -> nodes.WindowGroupingWithin:
        return nodes.WindowGroupingWithin.make(dim_list=list(dims))

    def among(self, *dims: nodes.FormulaItem) -> nodes.WindowGroupingAmong:
        return nodes.WindowGroupingAmong.make(dim_list=list(dims))

    def fixed(self, *dims: nodes.FormulaItem) -> nodes.FixedLodSpecifier:
        return nodes.FixedLodSpecifier.make(dim_list=list(dims))

    def inherited(self) -> nodes.InheritedLodSpecifier:
        return nodes.InheritedLodSpecifier()

    def include(self, *dims: nodes.FormulaItem) -> nodes.IncludeLodSpecifier:
        return nodes.IncludeLodSpecifier.make(dim_list=list(dims))

    def exclude(self, *dims: nodes.FormulaItem) -> nodes.ExcludeLodSpecifier:
        return nodes.ExcludeLodSpecifier.make(dim_list=list(dims))

    def asc(self, expr: nodes.FormulaItem) -> nodes.OrderAscending:
        return nodes.OrderAscending.make(expr=expr)

    def desc(self, expr: nodes.FormulaItem) -> nodes.OrderDescending:
        return nodes.OrderDescending.make(expr=expr)

    def _normalize_raw_bfb(
        self,
        before_filter_by: Optional[List[Union[nodes.Field, builtins.str]]] = None,
    ) -> nodes.BeforeFilterBy:
        return nodes.BeforeFilterBy.make(
            field_names=frozenset([f.name if isinstance(f, nodes.Field) else f for f in (before_filter_by or ())]),
        )

    class FuncShortcut:
        """SQLAlchemy-like function shortcut provider: ````"""

        def __init__(self, assume_window: bool = False):
            self._assume_window = assume_window

        def __getattr__(self, name: str):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
            return self._make_func(name)

        def __call__(self, name: str):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
            return self._make_func(name)

        def _make_func(self, name: str):  # type: ignore  # 2024-01-24 # TODO: Function is missing a return type annotation  [no-untyped-def]
            def wrapper(
                *pos_args: nodes.FormulaItem,
                grouping: Optional[nodes.WindowGrouping] = None,
                total: Optional[bool] = None,
                within: Optional[List[nodes.FormulaItem]] = None,
                among: Optional[List[nodes.FormulaItem]] = None,
                order_by: Optional[List[nodes.FormulaItem]] = None,
                before_filter_by: Optional[List[Union[nodes.Field, str]]] = None,
                ignore_dimensions: Optional[nodes.IgnoreDimensions] = None,
                lod: Optional[nodes.LodSpecifier] = None,
                args: Sequence[nodes.FormulaItem] = (),
                meta: Optional[nodes.NodeMeta] = None,
            ) -> Union[nodes.FuncCall, nodes.WindowFuncCall]:
                assert (
                    len([bool(spec) for spec in (grouping, total, within, among) if spec is not None]) <= 1
                ), "Only one grouping specifier is allowed"

                if total is not None:
                    assert total is True, "Invalid value for total"
                    grouping = n.total()
                elif within is not None:
                    grouping = n.within(*within)
                elif among is not None:
                    grouping = n.among(*among)

                if grouping is None and self._assume_window:
                    grouping = n.total()

                before_filter_by_node = n._normalize_raw_bfb(before_filter_by=before_filter_by)

                assert not (pos_args and args), "Can't specify pos args and `args` keyword simultaneously"
                args = pos_args or args or ()

                if grouping is None and order_by is None:
                    return nodes.FuncCall.make(
                        name=name.lower(),
                        args=[_norm(arg) for arg in args],
                        lod=lod,
                        ignore_dimensions=ignore_dimensions,
                        before_filter_by=before_filter_by_node,
                        meta=meta,
                    )

                return nodes.WindowFuncCall.make(
                    name=name.lower(),
                    args=[_norm(arg) for arg in args],
                    grouping=grouping,
                    ordering=nodes.Ordering.make(expr_list=order_by or []),
                    before_filter_by=before_filter_by_node,
                    meta=meta,
                )

            return wrapper

    func = FuncShortcut()  # n.func.SOME_FUNCTION(*args)
    wfunc = FuncShortcut(assume_window=True)  # same, but always results in a window function

    class WhenProxy:
        def __init__(self, val: Any) -> None:
            self._val = val

        def then(self, expr: Any) -> nodes.WhenPart:
            return nodes.WhenPart.make(val=_norm(self._val), expr=_norm(expr))

    def when(self, val: Any) -> "NodeShortcut.WhenProxy":
        return self.WhenProxy(val)

    class CaseProxy:
        def __init__(self, case_expr):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
            self._case_expr = case_expr
            self._when_list = []

        def whens(self, *when_list) -> "NodeShortcut.CaseProxy":  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
            self._when_list += when_list or []
            return self

        def else_(self, expr) -> nodes.CaseBlock:  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
            return nodes.CaseBlock.make(
                case_expr=_norm(self._case_expr), when_list=self._when_list, else_expr=_norm(expr)
            )

    def case(self, expr) -> "NodeShortcut.CaseProxy":  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
        """
        Usage:
        TODO
        """
        return self.CaseProxy(expr)

    class IfPartProxy:
        def __init__(self, cond):  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation  [no-untyped-def]
            self._cond = cond
            self._expr = None

        def then(self, expr) -> "NodeShortcut.IfPartProxy":  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
            self._expr = expr
            return self

        def else_(self, expr) -> nodes.IfBlock:  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
            return n.IfBlockProxy([self]).else_(expr)

    def elseif(self, cond) -> "NodeShortcut.IfPartProxy":  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
        return self.IfPartProxy(cond)

    class IfBlockProxy:
        def __init__(self, if_list: list):
            self._if_list = if_list

        def else_(self, expr) -> nodes.IfBlock:  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
            return nodes.IfBlock.make(
                if_list=[nodes.IfPart.make(cond=_norm(ipp._cond), expr=_norm(ipp._expr)) for ipp in self._if_list],
                else_expr=_norm(expr),
            )

    def if_(self, *exprs) -> Union["NodeShortcut.IfBlockProxy", "NodeShortcut.IfPartProxy"]:  # type: ignore  # 2024-01-24 # TODO: Function is missing a type annotation for one or more arguments  [no-untyped-def]
        """
        Usage:
        TODO
        """
        if len(exprs) > 1:
            return self.IfBlockProxy(list(exprs))
        if isinstance(exprs[0], self.IfPartProxy):
            return exprs[0]
        return self.IfPartProxy(exprs[0])

    def field(self, name: builtins.str) -> nodes.Field:
        return nodes.Field.make(name=name)

    def formula(self, expr: nodes.FormulaItem) -> nodes.Formula:
        return nodes.Formula.make(expr=_norm(expr))

    def p(self, expr: nodes.FormulaItem) -> nodes.ParenthesizedExpr:
        return nodes.ParenthesizedExpr.make(expr=_norm(expr))

    def tagged(self, tag: LevelTag, expr: nodes.FormulaItem) -> nodes.FormulaItem:
        return expr.with_tag(level_tag=tag)

    def level_tag(
        self,
        names: FrozenSet[builtins.str],
        nesting: builtins.int,
        qfork_nesting: builtins.int = 0,
    ) -> LevelTag:
        return LevelTag(bfb_names=names, func_nesting=nesting, qfork_nesting=qfork_nesting)

    def fork(
        self,
        joining: fork_nodes.QueryForkJoiningBase,
        result_expr: nodes.FormulaItem,
        before_filter_by: Optional[List[Union[nodes.Field, builtins.str]]] = None,
        lod: Optional[nodes.FixedLodSpecifier] = None,
        join_type: fork_nodes.JoinType = fork_nodes.JoinType.left,
        bfb_filter_mutations: Optional[fork_nodes.BfbFilterMutations] = None,
    ) -> fork_nodes.QueryFork:
        before_filter_by_node = self._normalize_raw_bfb(before_filter_by=before_filter_by)
        return fork_nodes.QueryFork.make(
            join_type=join_type,
            joining=joining,
            result_expr=result_expr,
            before_filter_by=before_filter_by_node,
            lod=lod,
            bfb_filter_mutations=bfb_filter_mutations,
        )

    def bin_condition(self, expr: nodes.FormulaItem, fork_expr: nodes.FormulaItem) -> fork_nodes.BinaryJoinCondition:
        return fork_nodes.BinaryJoinCondition.make(expr=expr, fork_expr=fork_expr)

    def self_condition(self, expr: nodes.FormulaItem) -> fork_nodes.SelfEqualityJoinCondition:
        return fork_nodes.SelfEqualityJoinCondition.make(expr=expr)

    def joining(
        self,
        *,
        conditions: Optional[Sequence[fork_nodes.JoinConditionBase]] = None,
    ) -> fork_nodes.QueryForkJoiningBase:
        if conditions is not None:
            return fork_nodes.QueryForkJoiningWithList.make(
                condition_list=list(conditions),
            )
        raise ValueError("Invalid options for joining object")

    def unary(
        self,
        name: builtins.str,
        expr: nodes.FormulaItem,
        meta: Optional[nodes.NodeMeta] = None,
    ) -> nodes.Unary:
        return nodes.Unary.make(name=name, expr=expr, meta=meta)

    def binary(
        self,
        name: builtins.str,
        left: nodes.FormulaItem,
        right: nodes.FormulaItem,
        meta: Optional[nodes.NodeMeta] = None,
    ) -> nodes.Binary:
        return nodes.Binary.make(name=name, left=left, right=right, meta=meta)

    def ternary(
        self,
        name: builtins.str,
        first: nodes.FormulaItem,
        second: nodes.FormulaItem,
        third: nodes.FormulaItem,
        meta: Optional[nodes.NodeMeta] = None,
    ) -> nodes.Ternary:
        return nodes.Ternary.make(name=name, first=first, second=second, third=third, meta=meta)

    def not_(
        self,
        expr: nodes.FormulaItem,
        meta: Optional[nodes.NodeMeta] = None,
    ) -> nodes.Unary:
        return self.unary("not", expr, meta=meta)


n = NodeShortcut()
