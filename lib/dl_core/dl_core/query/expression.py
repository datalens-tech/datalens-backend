from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Sequence,
)

import attr
from typing_extensions import Self

from dl_constants.enums import OrderDirection


if TYPE_CHECKING:
    from sqlalchemy.sql.elements import ClauseElement

    from dl_constants.enums import (
        JoinType,
        UserDataType,
    )
    from dl_core.components.ids import AvatarId


@attr.s(auto_attribs=True, frozen=True)
class ExpressionCtx:
    expression: ClauseElement
    avatar_ids: Optional[Sequence[str]] = None  # TODO: make required
    user_type: Optional[UserDataType] = None
    alias: Optional[str] = None
    original_field_id: Optional[Any] = None

    def clone(self, **kwargs: Any) -> Self:
        return attr.evolve(self, **kwargs)


@attr.s(auto_attribs=True, frozen=True)
class OrderByExpressionCtx(ExpressionCtx):
    direction: OrderDirection = OrderDirection.asc


@attr.s(auto_attribs=True, frozen=True)
class JoinOnExpressionCtx(ExpressionCtx):  # noqa
    left_id: Optional[AvatarId] = attr.ib(kw_only=True)  # can be None for feature-managed relations
    right_id: AvatarId = attr.ib(kw_only=True)
    join_type: JoinType = attr.ib(kw_only=True)
