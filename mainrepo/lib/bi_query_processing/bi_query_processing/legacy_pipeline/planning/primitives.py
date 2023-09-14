from __future__ import annotations

from enum import (
    Enum,
    unique,
)
from typing import (
    List,
    Optional,
    Sequence,
    Tuple,
)

import attr

from bi_constants.enums import JoinType
from bi_core.components.ids import AvatarId
from bi_formula.core.tag import LevelTag
from bi_query_processing.compilation.primitives import (
    CompiledFormulaInfo,
    JoinedFromObject,
)
from bi_query_processing.compilation.query_meta import QueryMetaInfo
from bi_query_processing.compilation.specs import OrderDirection
from bi_query_processing.enums import ExecutionLevel


@unique
class SlicerType(Enum):
    # Common types
    top = "top"
    window = "window"
    fields = "fields"
    level_tagged = "level_tagged"


@attr.s(slots=True, frozen=True)
class SlicerConfiguration:
    slicer_type: SlicerType = attr.ib(kw_only=True)


@attr.s(slots=True, frozen=True)
class TaggedSlicerConfiguration(SlicerConfiguration):
    tag: LevelTag = attr.ib(kw_only=True)


@attr.s(slots=True, frozen=True)
class LevelPlan:
    level_types: List[ExecutionLevel] = attr.ib(kw_only=True)

    @property
    def top_level_type(self) -> ExecutionLevel:
        return self.level_types[-1]

    @property
    def top_level_idx(self) -> int:
        return len(self.level_types) - 1

    def level_count(self, level_type: Optional[ExecutionLevel] = None) -> int:
        if level_type is None:
            return len(self.level_types)
        return self.level_types.count(level_type)

    def get_level_type(self, level_index: int) -> ExecutionLevel:
        return self.level_types[level_index]


@attr.s(slots=True, frozen=True)
class SlicingPlan:
    slicer_configs: Tuple[SlicerConfiguration, ...] = attr.ib(kw_only=True)

    def level_count(self) -> int:
        return len(self.slicer_configs)

    def get_level_slicer_config(self, level_index: int) -> SlicerConfiguration:
        return self.slicer_configs[level_index]

    def __add__(self, other: "SlicingPlan") -> "SlicingPlan":
        assert isinstance(other, SlicingPlan)
        return SlicingPlan(slicer_configs=self.slicer_configs + other.slicer_configs)


@attr.s(slots=True, frozen=True)
class PlannedFormula:
    formula: CompiledFormulaInfo = attr.ib(kw_only=True)
    level_plan: LevelPlan = attr.ib(kw_only=True)
    slicing_plan: SlicingPlan = attr.ib(kw_only=True)

    def __attrs_post_init__(self) -> None:
        self._validate()

    def _validate(self) -> None:
        assert self.level_plan.level_count() == self.slicing_plan.level_count()


@attr.s(slots=True, frozen=True)
class PlannedOrderByFormula(PlannedFormula):
    direction: OrderDirection = attr.ib()


@attr.s(slots=True, frozen=True)
class PlannedJoinOnFormula(PlannedFormula):
    left_id: Optional[AvatarId] = attr.ib(kw_only=True)  # can be None for feature-managed relations
    right_id: AvatarId = attr.ib(kw_only=True)
    join_type: JoinType = attr.ib(kw_only=True)


@attr.s(slots=True, frozen=True)
class ExecutionPlan:
    id: AvatarId = attr.ib(kw_only=True)
    level_plan: LevelPlan = attr.ib()

    select: Sequence[PlannedFormula] = attr.ib()
    # skipped: relation 'join_on'
    filters: Sequence[PlannedFormula] = attr.ib()
    group_by: Sequence[PlannedFormula] = attr.ib()
    order_by: Sequence[PlannedOrderByFormula] = attr.ib()
    join_on: List[PlannedJoinOnFormula] = attr.ib(kw_only=True)
    joined_from: JoinedFromObject = attr.ib(kw_only=True, factory=JoinedFromObject)
    limit: Optional[int] = attr.ib()
    offset: Optional[int] = attr.ib()
    meta: QueryMetaInfo = attr.ib(kw_only=True, factory=QueryMetaInfo)


# Common slicer configs
TOP_SLICER_CONFIG = SlicerConfiguration(slicer_type=SlicerType.top)
WINDOW_SLICER_CONFIG = SlicerConfiguration(slicer_type=SlicerType.window)
FIELD_SLICER_CONFIG = SlicerConfiguration(slicer_type=SlicerType.fields)
