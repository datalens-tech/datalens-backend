from __future__ import annotations

from typing import Dict, List, Optional, Set

import attr

from bi_constants.enums import JoinType

from bi_core.components.ids import AvatarId, FieldId

from bi_query_processing.compilation.query_meta import QueryMetaInfo
from bi_query_processing.compilation.primitives import CompiledFormulaInfo, JoinedFromObject

from bi_query_processing.compilation.specs import OrderDirection
from bi_query_processing.legacy_pipeline.planning.primitives import LevelPlan


@attr.s(slots=True, frozen=True)
class SliceFormulaSlice:
    aliased_formulas: Dict[str, CompiledFormulaInfo] = attr.ib(kw_only=True)
    required_fields: Set[str] = attr.ib(kw_only=True, factory=set)


@attr.s(slots=True, frozen=True)
class SlicedFormula:
    slices: List[SliceFormulaSlice] = attr.ib(kw_only=True)
    level_plan: LevelPlan = attr.ib(kw_only=True)
    original_field_id: Optional[FieldId] = attr.ib(kw_only=True, default=None)


@attr.s(slots=True, frozen=True)
class SlicedOrderByFormula(SlicedFormula):  # noqa
    direction: OrderDirection = attr.ib(kw_only=True)


@attr.s(slots=True, frozen=True)
class SlicedJoinOnFormula(SlicedFormula):  # noqa
    left_id: Optional[AvatarId] = attr.ib(kw_only=True)  # can be None for feature-managed relations
    right_id: AvatarId = attr.ib(kw_only=True)
    join_type: JoinType = attr.ib(kw_only=True)


@attr.s(slots=True, frozen=True)
class SlicedQuery:
    id: AvatarId = attr.ib(kw_only=True)
    level_plan: LevelPlan = attr.ib(kw_only=True)

    select: List[SlicedFormula] = attr.ib(kw_only=True)
    filters: List[SlicedFormula] = attr.ib(kw_only=True)
    group_by: List[SlicedFormula] = attr.ib(kw_only=True)
    order_by: List[SlicedOrderByFormula] = attr.ib(kw_only=True)
    join_on: List[SlicedJoinOnFormula] = attr.ib(kw_only=True)
    joined_from: JoinedFromObject = attr.ib(kw_only=True, factory=JoinedFromObject)
    limit: Optional[int] = attr.ib(kw_only=True)
    offset: Optional[int] = attr.ib(kw_only=True)
    meta: QueryMetaInfo = attr.ib(kw_only=True, factory=QueryMetaInfo)
