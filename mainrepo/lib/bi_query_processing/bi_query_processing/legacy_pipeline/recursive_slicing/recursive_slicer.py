from __future__ import annotations

import logging
from typing import (
    Any,
    Dict,
    List,
    Sequence,
)

import attr

from bi_query_processing.compilation.primitives import CompiledQuery
from bi_query_processing.enums import ExecutionLevel
from bi_query_processing.legacy_pipeline.planning.planner import ExecutionPlanner
from bi_query_processing.legacy_pipeline.separation.primitives import (
    CompiledLevel,
    CompiledMultiLevelQuery,
)
from bi_query_processing.legacy_pipeline.separation.separator import QuerySeparator
from bi_query_processing.legacy_pipeline.slicing.query_slicer import QuerySlicerBase

LOGGER = logging.getLogger(__name__)


@attr.s
class RecursiveQuerySlicer:
    _initial_planner: ExecutionPlanner = attr.ib(kw_only=True)
    _secondary_planners_by_level_type: Dict[ExecutionLevel, Sequence[ExecutionPlanner]] = attr.ib(kw_only=True)
    _query_slicer: QuerySlicerBase = attr.ib(kw_only=True)
    _query_separator: QuerySeparator = attr.ib(kw_only=True)
    _verbose_logging: bool = attr.ib(kw_only=True, default=False)
    # Internal attributes
    _iteration_counter: int = attr.ib(init=False, default=0)

    def _log_info(self, *args: Any, **kwargs: Any) -> None:
        if self._verbose_logging:
            LOGGER.info(*args, **kwargs)

    def _get_new_iteration_id(self) -> str:
        iteration_id = f"i{self._iteration_counter}"
        self._iteration_counter += 1
        return iteration_id

    def _slice_query_using_planner(
        self,
        *,
        compiled_query: CompiledQuery,
        query_planner: ExecutionPlanner,
    ) -> CompiledMultiLevelQuery:
        iteration_id = self._get_new_iteration_id()
        self._log_info(f"Starting slicing iteration {iteration_id} for query {compiled_query.id}")

        planned_query = query_planner.plan(compiled_query=compiled_query)
        self._log_info(f"Finished creating an execution plan on iteration {iteration_id} for query {compiled_query.id}")

        sliced_query = self._query_slicer.slice_query(planned_query=planned_query, iteration_id=iteration_id)
        self._log_info(f"Finished slicing query {compiled_query.id} on iteration {iteration_id}")

        compiled_multi_query = self._query_separator.separate_query(
            sliced_query=sliced_query, iteration_id=iteration_id
        )
        self._log_info(f"Finished separating query {compiled_query.id} on iteration {iteration_id}")
        self._log_info(f"Finished slicing iteration {iteration_id} for query {compiled_query.id}")
        return compiled_multi_query

    def _slice_levels_using_planner(
        self,
        *,
        compiled_levels: Sequence[CompiledLevel],
        query_planner: ExecutionPlanner,
    ) -> List[CompiledLevel]:
        if not compiled_levels:
            return []

        original_top_level_query_ids = {q.id for q in compiled_levels[-1].queries}
        original_used_avatar_ids = {
            from_id for query in compiled_levels[0].queries for from_id in query.joined_from.iter_ids()
        }

        new_levels: List[CompiledLevel] = []
        for compiled_level in compiled_levels:
            sub_multi_queries = [
                self._slice_query_using_planner(compiled_query=compiled_query, query_planner=query_planner)
                for compiled_query in compiled_level.queries
            ]

            # Make sure all sub-queries have the same number of sub-levels
            level_counts = {len(sub_multi_query.levels) for sub_multi_query in sub_multi_queries}
            assert len(level_counts) == 1, f"Got different level counts {level_counts} for sliced sub-queries"
            query_ids = {q.id for q in compiled_level.queries}
            sub_level_cnt = next(iter(level_counts))  # There is only one item
            if sub_level_cnt > 1:
                self._log_info(f"Split queries {query_ids} into {sub_level_cnt} levels")

            # Make sure all sub-queries have the same level_type as the parent level
            level_types = {lvl.level_type for sub_multi_query in sub_multi_queries for lvl in sub_multi_query.levels}
            assert len(level_types) == 1 and next(iter(level_types)) == compiled_level.level_type, (
                f"Level type mismatch. Original level type: {compiled_level.level_type};"
                f"got level types: {level_types}"
            )

            # Add levels from subqueries
            for sub_level_idx in range(sub_level_cnt):
                sub_level = CompiledLevel(
                    queries=[
                        query
                        for sub_multi_query in sub_multi_queries
                        for query in sub_multi_query.levels[sub_level_idx].queries
                    ],
                    level_type=compiled_level.level_type,
                )
                new_levels.append(sub_level)

        # Validate first-level used avatar ids
        new_used_avatar_ids = {from_id for q in compiled_levels[0].queries for from_id in q.joined_from.iter_ids()}
        assert new_used_avatar_ids == original_used_avatar_ids, (
            f"Avatar ID mismatch. Original set: ({original_used_avatar_ids});" f"new set ({new_used_avatar_ids})"
        )

        # Validate top-level query ids
        new_top_level_query_ids = {q.id for q in compiled_levels[-1].queries}
        assert new_top_level_query_ids == original_top_level_query_ids, (
            f"Query ID mismatch. Original set: ({original_top_level_query_ids});" f"new set ({new_top_level_query_ids})"
        )

        return new_levels

    def _slice_levels_recursively_for_level_type(
        self,
        *,
        compiled_levels: List[CompiledLevel],
        level_type: ExecutionLevel,
    ) -> List[CompiledLevel]:
        for query_planner in self._secondary_planners_by_level_type.get(level_type, ()):
            self._log_info(
                f"Sending {len(compiled_levels)} {level_type.name} levels " f"to planner {type(query_planner).__name__}"
            )
            compiled_levels = self._slice_levels_using_planner(
                compiled_levels=compiled_levels,
                query_planner=query_planner,
            )

        self._log_info(f"Got {len(compiled_levels)} levels after slicing " f"for level type {level_type.name}")

        return compiled_levels

    def slice_query_recursively(self, compiled_query: CompiledQuery) -> CompiledMultiLevelQuery:
        compiled_multi_query = self._slice_query_using_planner(
            compiled_query=compiled_query, query_planner=self._initial_planner
        )

        level_type_order = (ExecutionLevel.source_db, ExecutionLevel.compeng)
        all_new_compiled_levels: List[CompiledLevel] = []
        for level_type in level_type_order:
            old_compiled_levels_for_type = [lvl for lvl in compiled_multi_query.levels if lvl.level_type == level_type]
            new_compiled_levels_for_type = self._slice_levels_recursively_for_level_type(
                compiled_levels=old_compiled_levels_for_type, level_type=level_type
            )
            all_new_compiled_levels.extend(new_compiled_levels_for_type)

        for level_idx, compiled_level in enumerate(all_new_compiled_levels):
            level_query_ids = [q.id for q in compiled_level.queries]
            level_used_avatar_ids = {from_id for q in compiled_level.queries for from_id in q.joined_from.iter_ids()}
            self._log_info(f"Level {level_idx} query IDs: {level_query_ids}; used avatar IDs: {level_used_avatar_ids}")

        self._log_info(f"Final levels: {[lvl.level_type.name for lvl in all_new_compiled_levels]}")
        updated_compiled_multi_query = compiled_multi_query.clone(levels=all_new_compiled_levels)
        return updated_compiled_multi_query
