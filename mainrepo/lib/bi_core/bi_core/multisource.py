# TODO: Move to bi_core.components.*

from __future__ import annotations

from typing import List

import attr

from bi_constants.enums import (
    BinaryJoinOperator,
    ConditionPartCalcMode,
    JoinConditionType,
    JoinType,
    ManagedBy,
)
from bi_core.components.ids import (
    AvatarId,
    FieldId,
    RelationId,
    SourceId,
)


@attr.s(frozen=True)
class ConditionPart:
    calc_mode: ConditionPartCalcMode = attr.ib()


@attr.s(frozen=True)
class ConditionPartDirect(ConditionPart):
    """Condition part that references a column from avatar's raw schema"""

    source: str = attr.ib()
    calc_mode: ConditionPartCalcMode = attr.ib(default=ConditionPartCalcMode.direct)


@attr.s(frozen=True)
class ConditionPartFormula(ConditionPart):  # TODO: Remove
    """Condition part defined by a formula that references avatar's raw schema columns"""

    formula: str = attr.ib()
    calc_mode: ConditionPartCalcMode = attr.ib(default=ConditionPartCalcMode.formula)


@attr.s(frozen=True)
class ConditionPartResultField(ConditionPart):
    """Condition part that references a field from dataset's result schema"""

    field_id: FieldId = attr.ib()
    calc_mode: ConditionPartCalcMode = attr.ib(default=ConditionPartCalcMode.result_field)


@attr.s(frozen=True)
class JoinCondition:
    pass


@attr.s(frozen=True)
class BinaryCondition(JoinCondition):
    operator: BinaryJoinOperator = attr.ib()
    left_part: ConditionPart = attr.ib()
    right_part: ConditionPart = attr.ib()
    condition_type: JoinConditionType = attr.ib(default=JoinConditionType.binary)


@attr.s(slots=True)
class SourceAvatar:
    id: AvatarId = attr.ib()
    source_id: SourceId = attr.ib()
    title: str = attr.ib()
    is_root: bool = attr.ib()
    managed_by: ManagedBy = attr.ib(default=ManagedBy.user)
    valid: bool = attr.ib(default=True)


@attr.s(slots=True)
class AvatarRelation:
    id: RelationId = attr.ib()
    left_avatar_id: AvatarId = attr.ib()
    right_avatar_id: AvatarId = attr.ib()
    conditions: List[BinaryCondition] = attr.ib(factory=list)
    join_type: JoinType = attr.ib(default=JoinType.inner)
    managed_by: ManagedBy = attr.ib(default=ManagedBy.user)
    valid: bool = attr.ib(default=True)
