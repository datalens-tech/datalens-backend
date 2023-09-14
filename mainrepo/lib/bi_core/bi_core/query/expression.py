from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Sequence,
    TypeVar,
)

import attr

from bi_constants.enums import OrderDirection

if TYPE_CHECKING:
    from sqlalchemy.sql.elements import ClauseElement

    from bi_constants.enums import (
        BIType,
        JoinType,
    )
    from bi_core.components.ids import AvatarId


_EXPRESSION_CTX_TV = TypeVar("_EXPRESSION_CTX_TV", bound="ExpressionCtx")


@attr.s(auto_attribs=True, frozen=True)
class ExpressionCtx:
    expression: ClauseElement
    avatar_ids: Optional[Sequence[str]] = None  # TODO: make required
    user_type: Optional[BIType] = None
    alias: Optional[str] = None
    original_field_id: Optional[Any] = None

    def clone(self: _EXPRESSION_CTX_TV, **kwargs: Any) -> _EXPRESSION_CTX_TV:
        return attr.evolve(self, **kwargs)


@attr.s(auto_attribs=True, frozen=True)
class OrderByExpressionCtx(ExpressionCtx):
    direction: OrderDirection = OrderDirection.asc


@attr.s(auto_attribs=True, frozen=True)
class JoinOnExpressionCtx(ExpressionCtx):  # noqa
    left_id: Optional[AvatarId] = attr.ib(kw_only=True)  # can be None for feature-managed relations
    right_id: AvatarId = attr.ib(kw_only=True)
    join_type: JoinType = attr.ib(kw_only=True)
