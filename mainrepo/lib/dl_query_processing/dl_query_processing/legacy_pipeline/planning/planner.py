from __future__ import annotations

from itertools import chain
import logging
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Iterable,
    List,
    Sequence,
    Set,
)

import attr

from dl_core.components.ids import FieldId
from dl_core.us_dataset import Dataset
from dl_core.utils import attrs_evolve_to_subclass
import dl_formula.core.nodes as formula_nodes
from dl_formula.core.tag import LevelTag
from dl_formula.inspect.env import InspectionEnvironment
from dl_formula.inspect.expression import (
    collect_tags,
    is_aggregate_expression,
    is_window_expression,
)
from dl_query_processing.compilation.primitives import (
    CompiledJoinOnFormulaInfo,
    CompiledOrderByFormulaInfo,
)
from dl_query_processing.enums import QueryType
import dl_query_processing.exc
from dl_query_processing.legacy_pipeline.planning.primitives import (
    FIELD_SLICER_CONFIG,
    TOP_SLICER_CONFIG,
    WINDOW_SLICER_CONFIG,
    ExecutionLevel,
    ExecutionPlan,
    LevelPlan,
    PlannedFormula,
    PlannedJoinOnFormula,
    PlannedOrderByFormula,
    SlicerType,
    SlicingPlan,
    TaggedSlicerConfiguration,
)

if TYPE_CHECKING:
    from dl_query_processing.compilation.primitives import (
        CompiledFormulaInfo,
        CompiledQuery,
    )


LOGGER = logging.getLogger(__name__)


def _plan_to_level(
    formulas: Sequence[CompiledFormulaInfo],
    *,
    level_plan: LevelPlan,
    slicing_plan: SlicingPlan,
) -> List[PlannedFormula]:
    return [
        PlannedFormula(
            formula=formula,
            level_plan=level_plan,
            slicing_plan=slicing_plan,
        )
        for formula in formulas
    ]


def split_list(iterable, condition):  # type: ignore  # TODO: fix
    """
    Split list items into `(matching, non_matching)` by `cond(item)` callable.
    """
    matching = []
    non_matching = []
    for item in iterable:
        if condition(item):
            matching.append(item)
        else:
            non_matching.append(item)
    return matching, non_matching


@attr.s
class ExecutionPlanner:
    """..."""

    ds: Dataset = attr.ib(kw_only=True)
    inspect_env: InspectionEnvironment = attr.ib(kw_only=True)

    _verbose_logging: bool = attr.ib(kw_only=True, default=False)

    def _log_info(self, *args, **kwargs) -> None:  # type: ignore  # TODO: fix
        if self._verbose_logging:
            LOGGER.info(*args, **kwargs)

    def _all_formulas(self, compiled_query: CompiledQuery) -> Iterable[CompiledFormulaInfo]:
        return chain.from_iterable(
            (
                compiled_query.select,
                compiled_query.group_by,
                compiled_query.order_by,
                compiled_query.filters,
                compiled_query.join_on,
            )
        )

    def _collect_tags(self, compiled_query: CompiledQuery) -> AbstractSet[LevelTag]:
        """
        Collect BFB tags that wrap every window function call
        from all expressions in the query.
        """
        tags: Set[LevelTag] = set()
        for comp_formula in self._all_formulas(compiled_query=compiled_query):
            tags |= collect_tags(comp_formula.formula_obj)
        return tags

    def _is_window(self, formula_info: CompiledFormulaInfo) -> bool:
        """Convenience wrapper for `is_window_expression`"""
        return is_window_expression(
            formula_info.formula_obj,
            env=self.inspect_env,
        )

    def _is_aggregate(self, formula_info: CompiledFormulaInfo) -> bool:
        """Convenience wrapper for `is_aggregate_expression`"""
        return is_aggregate_expression(
            formula_info.formula_obj,
            env=self.inspect_env,
        )

    def plan(self, compiled_query: CompiledQuery) -> ExecutionPlan:
        """
        Plan the query for slicing and execution as a whole.
        """
        raise NotImplementedError


class WindowToCompengExecutionPlanner(ExecutionPlanner):
    """
    Planner that sends window functions to compeng
    and everything else to source_db.
    """

    def plan(self, compiled_query: CompiledQuery) -> ExecutionPlan:
        first_level_level_plan = LevelPlan(level_types=[ExecutionLevel.source_db])
        first_level_slicing_plan = SlicingPlan(slicer_configs=(TOP_SLICER_CONFIG,))

        second_level_level_plan = LevelPlan(level_types=[ExecutionLevel.source_db, ExecutionLevel.compeng])
        second_level_slicing_plan = SlicingPlan(slicer_configs=(WINDOW_SLICER_CONFIG, TOP_SLICER_CONFIG))

        def plan_formula(formula: CompiledFormulaInfo, force_compeng: bool = False) -> PlannedFormula:
            if force_compeng or self._is_window(formula_info=formula):
                return PlannedFormula(
                    level_plan=second_level_level_plan,
                    slicing_plan=second_level_slicing_plan,
                    formula=formula,
                )

            return PlannedFormula(
                level_plan=first_level_level_plan,
                slicing_plan=first_level_slicing_plan,
                formula=formula,
            )

        needs_compeng = any(self._is_window(formula) for formula in self._all_formulas(compiled_query))
        unordered_tags = self._collect_tags(compiled_query=compiled_query)
        bfb_filter_field_ids: Set[FieldId] = set(
            chain.from_iterable(
                (
                    tag.bfb_names
                    for tag in unordered_tags
                    if tag.func_nesting != 0  # Referenced by a window function -> compeng
                )
            )
        )

        def plan_select_formula(formula: CompiledFormulaInfo) -> PlannedFormula:
            return plan_formula(formula, force_compeng=needs_compeng)  # All SELECTs must go to the top level

        def plan_group_by_formula(formula: CompiledFormulaInfo) -> PlannedFormula:
            assert not self._is_window(formula), "GROUP BY formulas must be planned to source_db"
            return plan_formula(formula)

        def plan_filter_formula(formula: CompiledFormulaInfo) -> PlannedFormula:
            # Send fields referenced by BFB tags to compeng
            force_compeng = formula.original_field_id in bfb_filter_field_ids
            return plan_formula(formula, force_compeng=force_compeng)

        def plan_order_by_formula(formula: CompiledOrderByFormulaInfo) -> PlannedOrderByFormula:
            return attrs_evolve_to_subclass(
                cls=PlannedOrderByFormula,
                inst=plan_formula(formula, force_compeng=needs_compeng),
                direction=formula.direction,
            )

        def plan_join_on_formula(formula: CompiledJoinOnFormulaInfo) -> PlannedJoinOnFormula:
            return attrs_evolve_to_subclass(
                cls=PlannedJoinOnFormula,
                inst=plan_formula(formula),
                left_id=formula.left_id,
                right_id=formula.right_id,
                join_type=formula.join_type,
            )

        if needs_compeng:
            level_plan = second_level_level_plan
        else:
            level_plan = first_level_level_plan

        planned_select = [plan_select_formula(f) for f in compiled_query.select]
        planned_group_by = [plan_group_by_formula(f) for f in compiled_query.group_by]
        planned_order_by = [plan_order_by_formula(f) for f in compiled_query.order_by]
        planned_filters = [plan_filter_formula(f) for f in compiled_query.filters]
        planned_join_on = [plan_join_on_formula(f) for f in compiled_query.join_on]

        return ExecutionPlan(
            id=compiled_query.id,
            level_plan=level_plan,
            select=planned_select,
            group_by=planned_group_by,
            filters=planned_filters,
            order_by=planned_order_by,
            join_on=planned_join_on,
            joined_from=compiled_query.joined_from,
            limit=compiled_query.limit,
            offset=compiled_query.offset,
            meta=compiled_query.meta,
        )


@attr.s
class NestedLevelTagExecutionPlanner(ExecutionPlanner):  # noqa
    """
    Plans the slicing of queries for "regular" sources.

    Example:

    Original Query:
        select: [Order Date]
                [Group Sales]:                    SUM([Sales])
                [RSUM of Sales BFB]:              RSUM([Group Sales] BEFORE FILTER BY [Order Date])
                [MAVG of MAVG of RSUM of Sales]:  MAVG(MAVG(RSUM([Group Sales]), 50), 50)
                [RSUM of Sales non-BFB]:          RSUM([Group Sales])
        group_by: [Order Date]
        order_by: [Order Date]
        filters: [Order Date] > #2020-01-01#

    Fields with window functions after normalization and tagging:
        [RSUM of Sales BFB]:
            LevelTag({"Order Date"}, 0)
              '-> RSUM([Group Sales])
        [MAVG of MAVG of RSUM of Sales BFB]:
            LevelTag({}, -1)
              '-> MAVG((...), 50))
                        '-> LevelTag({}, 0)
                             '-> MAVG((...), 50))
                                        '-> LevelTag({"Order Date"}, 0)
                                             '-> RSUM([Group Sales])
        [RSUM of Sales non-BFB]:
            LevelTag({}, 0)
              |-> RSUM([Group Sales])

    Collected and Sorted Level Tags:
        whole query:                     [LevelTag({"Order Date"},0), LevelTag({},0), LevelTag({},-1)]
        [RSUM of Sales BFB]:             [LevelTag({"Order Date"},0)]
        [MAVG of MAVG of RSUM of Sales]: [LevelTag({"Order Date"},0), LevelTag({},0), LevelTag({},-1)]
        [RSUM of Sales non-BFB]:         [LevelTag({},0)]

    source_db levels: 1
    window function_levels (tag count): 3
    source_db levels: 4  # (one extra level for fltering)

    Slicer Plans:
        select: [Order Date]:                     [Win, Win, Win, Win, Top]
                [Group Sales]:                    [Win, Win, Win, Win, Top]
                [RSUM of Sales BFB]:              [Win, Tagged({"Order Date"},0), Win, Win, Top]
                [MAVG of MAVG of RSUM of Sales]:  [Win, Tagged({"Order Date"},0), Tagged({},0), Tagged({},-1), Top]
                [RSUM of Sales non-BFB]:          [Win, Win, Win, Tagged({},-1), Top]
        group_by: [Order Date]
        order_by: [Order Date]
        filters: [Order Date] > #2020-01-01#

    Slicing Results:
    (just for the interesting formulas)
        +===+===========+============================+=================================+=========================+=============================+
        | N |   type    |    [RSUM of Sales BFB]     | [MAVG of MAVG of RSUM of Sales] | [RSUM of Sales non-BFB] |     [Order Date] filter     |
        +===+===========+============================+=================================+=========================+=============================+
        | 0 | source_db | SUM([Sales])               | SUM([Sales])                    | SUM([Sales])            | [Order Date] > #2020-01-01# |
        +---+-----------+----------------------------+---------------------------------+-------------------------+-----------------------------+
        | 1 | compeng   | RSUM(... BFB [Order Date]) | RSUM(... BFB [Order Date])      | ...                     | ...                         |
        +---+-----------+----------------------------+---------------------------------+-------------------------+-----------------------------+
        | 2 | compeng   | ...                        | MAVG(..., 50)                   | ...                     | ...                         |
        +---+-----------+----------------------------+---------------------------------+-------------------------+-----------------------------+
        | 3 | compeng   | ...                        | MAVG(..., 50)                   | RSUM(...)               | ...                         |
        +---+-----------+----------------------------+---------------------------------+-------------------------+-----------------------------+
        | 4 | compeng   | ...                        | ...                             | ...                     | WHERE ...                   |
        +---+-----------+----------------------------+---------------------------------+-------------------------+-----------------------------+
    """

    level_type: ExecutionLevel = attr.ib(kw_only=True)

    def _get_query_level_plan(
        self,
        level_tags: List[LevelTag],
    ) -> LevelPlan:
        """
        Determine the query's execution levels,
        """

        tag_level_count = len(level_tags)
        execution_level_count = tag_level_count + 1  # One extra level for possible filters

        level_types = [self.level_type] * execution_level_count

        self._log_info("Found %s tag levels", tag_level_count)
        self._log_info("Using %s execution levels", execution_level_count)
        self._log_info("All level types: %s", level_types)

        return LevelPlan(level_types=level_types)

    def _validate_and_order_tags(self, tags: AbstractSet[LevelTag]) -> List[LevelTag]:
        """
        Sort the given BFB tags in decreasing order:
        1. .names - from subset to superset
        2. .nesting - from lower to higher

        Validate that tags go in order of nesting:
        - correct: ({A,B,C,D}, 0), ({A,B}, 0), ({A}, 1), ({A}, 0)
        - incorrect: ({A,B}, 0), ({A}, 0), ({B}, 0)
        """
        assert all(isinstance(tag, LevelTag) for tag in tags)
        result: List[LevelTag] = sorted(tags, reverse=True)  # type: ignore
        # Make sure they are ordered correctly
        # LevelTag ordering itself doesn't raise errors if two items cannot be ordered correctly
        # (e.g. sets {a, b} and {c} - neither is a subset of the other)
        for i in range(1, len(result)):
            if not (result[i - 1] > result[i]):
                raise dl_query_processing.exc.UnresolvableSlicingTagOrder()
        return result

    def _get_aggregation_level(self) -> int:
        return 0  # always lowest level

    def _get_join_on_level(self) -> int:
        return 0  # always the first level

    def _get_level_for_filter(
        self,
        formula: CompiledFormulaInfo,
        level_tags: List[LevelTag],
    ) -> int:
        """
        Cases:
            1. Unknown filter -> default level (unless other cases apply)
            2. Filter is neither referenced by any BFB clause nor contains a BFB function -> default level
            3. Filter is referenced by a BFB clause -> its level is defined by that BFB clause (same level)
            4. Filter contains BFB functions -> level is defined by the outermost level tag in the filter expression
            5. Filter is both referenced by some BFB and itself contains BFB functions -> max(case 3, case 4)

        Basically 1 & 2 are just the default. We find values for cases 3 & 4
        and then return the maximum of all values.
        """

        base_execution_level = 0
        filter_level = base_execution_level

        # Case 3: get the max BFB reference level
        filter_field_id = formula.original_field_id
        if filter_field_id is not None:
            # Find the last tag containing the filter's ID
            tag_relative_level = len(level_tags)
            while tag_relative_level > 0:
                tag = level_tags[tag_relative_level - 1]
                if filter_field_id in tag.bfb_names:
                    bfb_ref_level = base_execution_level + tag_relative_level
                    self._log_info(
                        "Filter based on field %s is referenced in tag %s, which results in level %s",
                        filter_field_id,
                        tag,
                        bfb_ref_level,
                    )

                    if tag.qfork_nesting != 0:
                        # The top BFB reference for this filter is a qfork.
                        # For query forks BFB-referenced filter should be below the QueryFork node
                        bfb_ref_level = max(0, bfb_ref_level - 1)

                    filter_level = max(filter_level, bfb_ref_level)
                    break
                tag_relative_level -= 1

        # Case 4: if filter depends on window functions directly
        filter_tags = collect_tags(formula.formula_obj)
        if filter_tags:
            top_filter_tag = min(filter_tags)
            filter_func_dep_level = base_execution_level + level_tags.index(top_filter_tag) + 1
            self._log_info(
                "Filter based on field %s contains node with tag %s, which results in level %s",
                filter_field_id,
                top_filter_tag,
                filter_func_dep_level,
            )
            filter_level = max(filter_level, filter_func_dep_level)

        if filter_field_id is not None:
            self._log_info(f"Level of filter based on field {filter_field_id} was determined to be {filter_level}")

        return filter_level

    def _get_formula_slicing_plan(
        self, formula: CompiledFormulaInfo, level_plan: LevelPlan, before_level: int, level_tags: List[LevelTag]
    ) -> SlicingPlan:
        """
        Define how to slice the part of the formula that is executed in in ``self.level_type``.
        Slicing is defined by BFB (BEFORE FILTER BY) tags
        that act as wrappers for all BFB functions.
        """
        assert all(lt == self.level_type for lt in level_plan.level_types[:before_level])

        levels_in_zone = before_level
        assert levels_in_zone != 0, "Cannot plan to 0 levels"

        slicer_configs = []

        for level_tag in level_tags[: levels_in_zone - 1]:  # -1 because of the extra filter level
            level_boundary_config = TaggedSlicerConfiguration(
                slicer_type=SlicerType.level_tagged,
                tag=level_tag,
            )
            slicer_configs.append(level_boundary_config)

        # Top everything off with a TOP_SLICER_CONFIG
        slicer_configs.append(TOP_SLICER_CONFIG)  # type: ignore  # TODO: fix
        return SlicingPlan(slicer_configs=tuple(slicer_configs))

    def _plan_formula(
        self,
        formula: CompiledFormulaInfo,
        level_plan: LevelPlan,
        before_level: int,
        level_tags: List[LevelTag],
    ) -> PlannedFormula:
        formula_slicing_plan = self._get_formula_slicing_plan(
            formula=formula, level_plan=level_plan, before_level=before_level, level_tags=level_tags
        )
        formula_level_plan = LevelPlan(level_types=level_plan.level_types[:before_level])
        return PlannedFormula(
            formula=formula,
            level_plan=formula_level_plan,
            slicing_plan=formula_slicing_plan,
        )

    def _plan_order_by_formula(
        self,
        formula: CompiledOrderByFormulaInfo,
        level_plan: LevelPlan,
        before_level: int,
        level_tags: List[LevelTag],
    ) -> PlannedOrderByFormula:
        assert isinstance(formula, CompiledOrderByFormulaInfo)
        planned_formula = self._plan_formula(
            formula=formula, level_plan=level_plan, before_level=before_level, level_tags=level_tags
        )
        return attrs_evolve_to_subclass(
            cls=PlannedOrderByFormula,
            inst=planned_formula,
            direction=formula.direction,
        )

    def _plan_join_on_formula(
        self,
        formula: CompiledJoinOnFormulaInfo,
        level_plan: LevelPlan,
        before_level: int,
        level_tags: List[LevelTag],
    ) -> PlannedJoinOnFormula:
        assert isinstance(formula, CompiledJoinOnFormulaInfo)
        planned_formula = self._plan_formula(
            formula=formula, level_plan=level_plan, before_level=before_level, level_tags=level_tags
        )
        return attrs_evolve_to_subclass(
            cls=PlannedJoinOnFormula,
            inst=planned_formula,
            left_id=formula.left_id,
            right_id=formula.right_id,
            join_type=formula.join_type,
        )

    def plan(self, compiled_query: CompiledQuery) -> ExecutionPlan:
        unordered_tags = self._collect_tags(compiled_query=compiled_query)
        level_tags = self._validate_and_order_tags(tags=unordered_tags)
        self._log_info("Using level tags for slicing: %s", level_tags)
        level_plan = self._get_query_level_plan(level_tags=level_tags)

        select_level = level_plan.level_count() - 1
        order_by_level = select_level
        group_by_level = self._get_aggregation_level()
        join_on_level = self._get_join_on_level()

        # Plan filters
        planned_filters: List[PlannedFormula] = []
        for filter_formula in compiled_query.filters:
            filter_level = self._get_level_for_filter(formula=filter_formula, level_tags=level_tags)
            planned_filter = self._plan_formula(
                formula=filter_formula, level_plan=level_plan, before_level=filter_level + 1, level_tags=level_tags
            )
            planned_filters.append(planned_filter)

        def _plan_regular_formula(formula: CompiledFormulaInfo, before_level: int) -> PlannedFormula:
            return self._plan_formula(
                formula=formula, level_plan=level_plan, before_level=before_level, level_tags=level_tags
            )

        planned_select = [
            _plan_regular_formula(formula, before_level=select_level + 1) for formula in compiled_query.select
        ]
        planned_group_by = [
            _plan_regular_formula(formula, before_level=group_by_level + 1) for formula in compiled_query.group_by
        ]
        planned_order_by = [
            self._plan_order_by_formula(
                formula=formula, level_plan=level_plan, before_level=order_by_level + 1, level_tags=level_tags
            )
            for formula in compiled_query.order_by
        ]
        planned_join_on = [
            self._plan_join_on_formula(
                formula=formula, level_plan=level_plan, before_level=join_on_level + 1, level_tags=level_tags
            )
            for formula in compiled_query.join_on
        ]

        return ExecutionPlan(
            id=compiled_query.id,
            level_plan=level_plan,
            select=planned_select,
            group_by=planned_group_by,
            filters=planned_filters,
            order_by=planned_order_by,
            join_on=planned_join_on,
            joined_from=compiled_query.joined_from,
            limit=compiled_query.limit,
            offset=compiled_query.offset,
            meta=compiled_query.meta,
        )


@attr.s
class PrefilterAndCompengExecutionPlanner(ExecutionPlanner):
    """
    Synopsis:

      * simple (lazy) fields and `_expr_is_prefilter`-compatible expressions
        are sent to `source_db`.
      * Anything else (and any `grouping` present) toggles `compeng`.
      * Further logic (`window_filter` / `compeng_secondary`) is as usual.
    """

    def _expr_is_prefilter(self, formula_info: CompiledFormulaInfo) -> bool:
        raise NotImplementedError

    def _expr_requires_compeng(self, expr: formula_nodes.FormulaItem) -> bool:
        # See also: `dl_formula.slicing.slicer.LevelSlice.is_lazy`
        if isinstance(expr, formula_nodes.Field):
            return False
        return True

    def _is_prefilter(self, formula_info: CompiledFormulaInfo, bfb_filter_ids: AbstractSet[FieldId]) -> bool:
        if formula_info.original_field_id in bfb_filter_ids:
            return False
        return self._expr_is_prefilter(formula_info)

    def _is_simple_query(self, compiled_query: CompiledQuery, compeng_only_filters: List[CompiledFormulaInfo]):  # type: ignore  # TODO: fix
        if (
            not compiled_query.order_by
            and not compiled_query.group_by
            and not compiled_query.join_on
            and not compeng_only_filters
            and not any(self._expr_requires_compeng(formula.formula_obj.expr) for formula in compiled_query.select)
        ):
            return True
        return False

    def _get_needs_compeng(
        self, compiled_query: CompiledQuery, compeng_only_filters: List[CompiledFormulaInfo]
    ) -> bool:
        if compiled_query.meta.query_type in (QueryType.value_range, QueryType.distinct):
            return False
        return not self._is_simple_query(compiled_query=compiled_query, compeng_only_filters=compeng_only_filters)

    def plan(self, compiled_query: CompiledQuery) -> ExecutionPlan:
        tags = self._collect_tags(compiled_query=compiled_query)
        bfb_filter_ids: Set[FieldId] = {
            filter_field_id for tag in tags if isinstance(tag, LevelTag) for filter_field_id in tag.bfb_names
        }

        pre_filters: List[CompiledFormulaInfo] = []
        compeng_only_filters: List[CompiledFormulaInfo] = []
        for filter_formula in compiled_query.filters:
            if self._is_prefilter(filter_formula, bfb_filter_ids=bfb_filter_ids):
                pre_filters.append(filter_formula)
            else:
                compeng_only_filters.append(filter_formula)

        needs_compeng = self._get_needs_compeng(
            compiled_query=compiled_query,
            compeng_only_filters=compeng_only_filters,
        )
        if not needs_compeng:
            pre_filters += compeng_only_filters
            compeng_only_filters = []

        first_level_level_plan = LevelPlan(level_types=[ExecutionLevel.source_db])
        first_level_slicing_plan = SlicingPlan(slicer_configs=(TOP_SLICER_CONFIG,))

        second_level_level_plan = LevelPlan(level_types=[ExecutionLevel.source_db, ExecutionLevel.compeng])
        second_level_slicing_plan = SlicingPlan(slicer_configs=(FIELD_SLICER_CONFIG, TOP_SLICER_CONFIG))

        def plan_formula(formula: CompiledFormulaInfo, use_compeng: bool) -> PlannedFormula:
            if use_compeng:
                return PlannedFormula(
                    level_plan=second_level_level_plan,
                    slicing_plan=second_level_slicing_plan,
                    formula=formula,
                )

            return PlannedFormula(
                level_plan=first_level_level_plan,
                slicing_plan=first_level_slicing_plan,
                formula=formula,
            )

        def plan_select_formula(formula: CompiledFormulaInfo) -> PlannedFormula:
            return plan_formula(formula, use_compeng=needs_compeng)  # All SELECTs must go to the top level

        def plan_group_by_formula(formula: CompiledFormulaInfo) -> PlannedFormula:
            assert not self._is_aggregate(formula), "GROUP BY formulas must cannot have aggregations"
            assert needs_compeng, "COMPENG is required for GROUP BY"
            return plan_formula(formula, use_compeng=True)

        def plan_order_by_formula(formula: CompiledOrderByFormulaInfo) -> PlannedOrderByFormula:
            return attrs_evolve_to_subclass(  # All ORDER BYs must go to the top level
                cls=PlannedOrderByFormula,
                inst=plan_formula(formula, use_compeng=needs_compeng),
                direction=formula.direction,
            )

        def plan_join_on_formula(formula: CompiledJoinOnFormulaInfo) -> PlannedJoinOnFormula:
            assert needs_compeng, "COMPENG is required for JOINs"
            return attrs_evolve_to_subclass(
                cls=PlannedJoinOnFormula,
                inst=plan_formula(formula, use_compeng=True),
                left_id=formula.left_id,
                right_id=formula.right_id,
                join_type=formula.join_type,
            )

        all_compeng_filters: List[CompiledFormulaInfo]
        if needs_compeng:
            # Duplicate pre-filters into compeng just in case
            all_compeng_filters = pre_filters + compeng_only_filters
        else:
            assert not compeng_only_filters, "There shouldn't be any compeng filters"
            all_compeng_filters = []

        planned_select = [plan_select_formula(f) for f in compiled_query.select]
        planned_group_by = [plan_group_by_formula(f) for f in compiled_query.group_by]
        planned_order_by = [plan_order_by_formula(f) for f in compiled_query.order_by]
        planned_filters = [plan_formula(f, use_compeng=False) for f in pre_filters] + [
            plan_formula(f, use_compeng=True) for f in all_compeng_filters
        ]
        planned_join_on = [plan_join_on_formula(f) for f in compiled_query.join_on]

        if needs_compeng:
            level_plan = second_level_level_plan
        else:
            level_plan = first_level_level_plan

        plan = ExecutionPlan(
            id=compiled_query.id,
            level_plan=level_plan,
            select=planned_select,
            group_by=planned_group_by,
            filters=planned_filters,
            order_by=planned_order_by,
            join_on=planned_join_on,
            joined_from=compiled_query.joined_from,
            limit=compiled_query.limit,
            offset=compiled_query.offset,
            meta=compiled_query.meta,
        )
        return plan
