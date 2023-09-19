from __future__ import annotations

import abc
import logging

import attr

from dl_core.utils import attrs_evolve_to_subclass
from dl_formula.inspect.env import InspectionEnvironment
from dl_query_processing.compilation.primitives import CompiledFormulaInfo
from dl_query_processing.legacy_pipeline.planning.primitives import (
    ExecutionPlan,
    PlannedFormula,
    PlannedJoinOnFormula,
    PlannedOrderByFormula,
)
from dl_query_processing.legacy_pipeline.slicing.factory import (
    SlicerFactory,
    SlicerFactoryBase,
)
from dl_query_processing.legacy_pipeline.slicing.primitives import (
    SlicedFormula,
    SlicedJoinOnFormula,
    SlicedOrderByFormula,
    SlicedQuery,
    SliceFormulaSlice,
)


LOGGER = logging.getLogger(__name__)


@attr.s
class QuerySlicerBase(abc.ABC):
    _factory: SlicerFactoryBase = attr.ib(kw_only=True)
    _debug_mode: bool = attr.ib(kw_only=True, default=False)

    def slice_formula(self, planned_formula: PlannedFormula, top_level_idx: int, iteration_id: str) -> SlicedFormula:
        if self._debug_mode:
            LOGGER.info(f"Getting slicer for plan {planned_formula.slicing_plan} on slicing iteration {iteration_id}")
        slicer = self._factory.get_slicer(
            slicing_plan=planned_formula.slicing_plan,
            iteration_id=iteration_id,
        )

        if self._debug_mode:
            LOGGER.info(f"Slicing formula {planned_formula.formula.pretty()} on slicing iteration {iteration_id}")
        raw_slices = slicer.slice_formula(formula=planned_formula.formula.formula_obj).slices
        slices = []
        for level_idx, raw_slice in enumerate(raw_slices):
            is_top_level = level_idx == top_level_idx
            aliased_formulas = {}
            for piece_alias, piece_formula in raw_slice.aliased_nodes.items():
                if is_top_level:
                    assert len(raw_slice.aliased_nodes) == 1, "There can be only one node at the top level"
                    # Set original alias for top-level expressions
                    piece_alias = planned_formula.formula.alias  # type: ignore  # TODO: fix
                aliased_formulas[piece_alias] = CompiledFormulaInfo(
                    alias=piece_alias,
                    formula_obj=piece_formula,
                    original_field_id=planned_formula.formula.original_field_id,
                    avatar_ids=planned_formula.formula.avatar_ids,
                )
            slices.append(
                SliceFormulaSlice(aliased_formulas=aliased_formulas, required_fields=raw_slice.required_fields)
            )

        return SlicedFormula(
            slices=slices,
            level_plan=planned_formula.level_plan,
            original_field_id=planned_formula.formula.original_field_id,
        )

    def slice_order_by_formula(
        self,
        planned_formula: PlannedOrderByFormula,
        top_level_idx: int,
        iteration_id: str,
    ) -> SlicedOrderByFormula:
        assert isinstance(planned_formula, PlannedOrderByFormula)
        sliced_formula = self.slice_formula(
            planned_formula=planned_formula, top_level_idx=top_level_idx, iteration_id=iteration_id
        )
        return attrs_evolve_to_subclass(
            cls=SlicedOrderByFormula,
            inst=sliced_formula,
            direction=planned_formula.direction,
        )

    def slice_join_on_formula(
        self,
        planned_formula: PlannedJoinOnFormula,
        top_level_idx: int,
        iteration_id: str,
    ) -> SlicedJoinOnFormula:
        assert isinstance(planned_formula, PlannedJoinOnFormula)
        sliced_formula = self.slice_formula(
            planned_formula=planned_formula, top_level_idx=top_level_idx, iteration_id=iteration_id
        )
        return attrs_evolve_to_subclass(
            cls=SlicedJoinOnFormula,
            inst=sliced_formula,
            left_id=planned_formula.left_id,
            right_id=planned_formula.right_id,
            join_type=planned_formula.join_type,
        )

    def slice_query(self, planned_query: ExecutionPlan, iteration_id: str) -> SlicedQuery:
        top_level_idx = planned_query.level_plan.level_count() - 1

        sliced_query = SlicedQuery(
            id=planned_query.id,
            level_plan=planned_query.level_plan,
            select=[
                self.slice_formula(formula, top_level_idx=top_level_idx, iteration_id=iteration_id)
                for formula in planned_query.select
            ],
            group_by=[
                self.slice_formula(formula, top_level_idx=top_level_idx, iteration_id=iteration_id)
                for formula in planned_query.group_by
            ],
            order_by=[
                self.slice_order_by_formula(formula, top_level_idx=top_level_idx, iteration_id=iteration_id)
                for formula in planned_query.order_by
            ],
            filters=[
                self.slice_formula(formula, top_level_idx=top_level_idx, iteration_id=iteration_id)
                for formula in planned_query.filters
            ],
            join_on=[
                self.slice_join_on_formula(formula, top_level_idx=top_level_idx, iteration_id=iteration_id)
                for formula in planned_query.join_on
            ],
            joined_from=planned_query.joined_from,
            limit=planned_query.limit,
            offset=planned_query.offset,
            meta=planned_query.meta,
        )
        return sliced_query


class DefaultQuerySlicer(QuerySlicerBase):
    def __init__(self, inspect_env: InspectionEnvironment, debug_mode: bool):
        super().__init__(factory=SlicerFactory(inspect_env=inspect_env), debug_mode=debug_mode)
