from __future__ import annotations

import itertools
from typing import AbstractSet, List, Optional, Sequence

from bi_core.components.ids import AvatarId

from bi_formula.inspect.env import InspectionEnvironment
from bi_formula.inspect.expression import is_constant_expression

from bi_query_processing.compilation.primitives import (
    CompiledQuery, CompiledFormulaInfo, CompiledJoinOnFormulaInfo,
    JoinedFromObject,
)
from bi_query_processing.legacy_pipeline.separation.primitives import CompiledMultiLevelQuery, CompiledLevel
from bi_query_processing.legacy_pipeline.subqueries.query_tools import (
    get_formula_list_used_avatar_ids, get_query_level_used_fields,
)


class MultiQuerySanitizer:
    """
    Cleans up:
    - unused SELECT expressions in subqueries
    - unused avatars from JOIN clauses
    """

    def _find_rightmost_joined_avatars(
            self, join_on_list: Sequence[CompiledJoinOnFormulaInfo]
    ) -> AbstractSet[AvatarId]:
        right_avatar_ids = {join_on_formula.right_id for join_on_formula in join_on_list}
        for join_on_formula in join_on_list:
            right_avatar_ids.discard(join_on_formula.left_id)
        return right_avatar_ids

    def _remove_unused_avatars_from_join_on(
            self, non_join_formulas: Sequence[CompiledFormulaInfo],
            join_on_list: List[CompiledJoinOnFormulaInfo],
    ) -> List[CompiledJoinOnFormulaInfo]:

        used_avatar_ids = get_formula_list_used_avatar_ids(formulas=non_join_formulas)

        # Keep removing the rightmost avatars that are not used
        while True:
            rightmost_avatars = self._find_rightmost_joined_avatars(join_on_list=join_on_list)
            remove_avatar_ids = rightmost_avatars - used_avatar_ids  # unused rightmost avatars
            if not remove_avatar_ids:
                break

            join_on_list = [
                join_on_formula for join_on_formula in join_on_list
                if join_on_formula.right_id not in remove_avatar_ids
            ]

        return join_on_list

    def sanitize_flat_query(
            self, compiled_query: CompiledQuery, is_bottom_level: bool, used_aliases: Optional[AbstractSet[str]],
            sanitize_select: bool = True, sanitize_join_on: bool = True,
    ) -> Optional[CompiledQuery]:

        inspect_env = InspectionEnvironment()

        # Remove unnecessary SELECT items
        new_select: List[CompiledFormulaInfo] = compiled_query.select
        if sanitize_select:
            assert used_aliases is not None
            new_select = [
                formula
                for formula in compiled_query.select
                if formula.alias in used_aliases
            ]

            if not new_select:
                # Remove this query completely
                return None

        only_constants = all(
            is_constant_expression(formula.formula_obj.expr, env=inspect_env)
            for formula in new_select
        )

        new_group_by = compiled_query.group_by
        if new_group_by and only_constants:
            # Only constants are selected, so GROUP BY is pointless.
            # Non-empty GROUP By will lead to there being multiple rows instead of just one
            # because an avatar will be used
            new_group_by = []

        # Unchanged:
        new_order_by = compiled_query.order_by

        # Remove unnecessary filters
        new_filters: List[CompiledFormulaInfo] = compiled_query.filters
        if new_filters:
            if only_constants and not new_group_by:
                # Only constants are selected, so remove all filters.
                # (same as for GROUP BY)
                new_filters = []

        # Remove unnecessary JOINs
        new_join_on: List[CompiledJoinOnFormulaInfo] = compiled_query.join_on
        if sanitize_join_on and compiled_query.join_on:
            non_join_formulas: Sequence[CompiledFormulaInfo] = tuple(itertools.chain(
                new_select, new_group_by,
                new_filters, new_order_by
            ))
            new_join_on = self._remove_unused_avatars_from_join_on(
                non_join_formulas=non_join_formulas,
                join_on_list=compiled_query.join_on,
            )

        new_all_formulas = (
            new_select + new_group_by + new_order_by  # type: ignore
            + new_filters + new_join_on  # type: ignore
        )
        used_avatar_ids = get_formula_list_used_avatar_ids(formulas=new_all_formulas)
        old_used_avatar_ids = set(compiled_query.joined_from.iter_ids())

        # Return as-is if there are no changes
        if (
                len(new_select) == len(compiled_query.select)
                and len(new_group_by) == len(compiled_query.group_by)
                and len(new_order_by) == len(compiled_query.order_by)
                and len(new_filters) == len(compiled_query.filters)
                and len(new_join_on) == len(compiled_query.join_on)
                and used_avatar_ids == old_used_avatar_ids
        ):
            return compiled_query

        # Resolve the root avatar/FROM id
        root_from_id: Optional[str]
        if not used_avatar_ids:
            if not is_bottom_level:
                root_from_id = None
            else:
                root_from_id = compiled_query.joined_from.root_from_id
        elif new_join_on:
            leftmost_ids = {jo.left_id for jo in new_join_on} - {jo.right_id for jo in new_join_on}
            assert len(leftmost_ids) == 1
            root_from_id = next(iter(leftmost_ids))
        else:
            root_from_id = next(iter(
                from_obj.id for from_obj
                in compiled_query.joined_from.froms if from_obj.id in used_avatar_ids
            ))

        joined_from = JoinedFromObject(
            root_from_id=root_from_id,
            froms=[from_obj for from_obj in compiled_query.joined_from.froms if from_obj.id in used_avatar_ids],
        )

        return compiled_query.clone(
            select=new_select, group_by=new_group_by, order_by=new_order_by,
            filters=new_filters, join_on=new_join_on,
            joined_from=joined_from,
        )

    def sanitize_compiled_level(
            self, compiled_level: CompiledLevel, used_aliases: Optional[AbstractSet[str]],
            sanitize_select: bool, is_bottom_level: bool,
    ) -> CompiledLevel:
        updated_level_queries: List[CompiledQuery] = []
        changed_queries = False
        for compiled_query in compiled_level.queries:
            updated_query = self.sanitize_flat_query(
                compiled_query=compiled_query, is_bottom_level=is_bottom_level,
                used_aliases=used_aliases,
                sanitize_select=sanitize_select, sanitize_join_on=True,
            )
            if updated_query is not compiled_query:
                changed_queries = True
            if updated_query is not None:
                updated_level_queries.append(updated_query)

        if changed_queries:
            compiled_level = compiled_level.clone(queries=updated_level_queries)

        return compiled_level

    def sanitize_multi_query(self, compiled_multi_query: CompiledMultiLevelQuery) -> CompiledMultiLevelQuery:
        updated_levels: List[CompiledLevel] = []
        changed_levels = False
        for level_idx in reversed(range(len(compiled_multi_query.levels))):
            compiled_level = compiled_multi_query.levels[level_idx]

            used_aliases: Optional[AbstractSet[str]]  # Aliases used by the next query level
            if level_idx == compiled_multi_query.level_count() - 1:
                sanitize_select = False
                used_aliases = None
            else:
                sanitize_select = True
                next_compiled_level = updated_levels[0]
                used_aliases = get_query_level_used_fields(next_compiled_level)

            updated_level = self.sanitize_compiled_level(
                compiled_level=compiled_level, used_aliases=used_aliases,
                sanitize_select=sanitize_select, is_bottom_level=level_idx == 0,
            )
            if updated_level is not compiled_level:
                changed_levels = True
            updated_levels.insert(0, updated_level)

        if changed_levels:
            compiled_multi_query = compiled_multi_query.clone(levels=updated_levels)

        return compiled_multi_query
