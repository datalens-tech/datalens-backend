from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
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
    avatar_ids: Sequence[str] | None = None  # TODO: make required
    user_type: UserDataType | None = None
    alias: str | None = None
    original_field_id: Any | None = None

    def clone(self, **kwargs: Any) -> Self:
        return attr.evolve(self, **kwargs)


@attr.s(auto_attribs=True, frozen=True)
class OrderByExpressionCtx(ExpressionCtx):
    direction: OrderDirection = OrderDirection.asc


@attr.s(auto_attribs=True, frozen=True)
class JoinOnExpressionCtx(ExpressionCtx):  # noqa
    left_id: AvatarId | None = attr.ib(kw_only=True)  # can be None for feature-managed relations
    right_id: AvatarId = attr.ib(kw_only=True)
    join_type: JoinType = attr.ib(kw_only=True)
