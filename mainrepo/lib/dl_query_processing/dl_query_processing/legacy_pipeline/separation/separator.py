from __future__ import annotations

import logging
from typing import (
    Dict,
    List,
    Optional,
    Set,
    TypeVar,
)

import attr

from dl_core.components.ids import AvatarId
from dl_core.constants import DataAPILimits
from dl_core.utils import attrs_evolve_to_subclass
from dl_formula.collections import NodeValueMap
from dl_formula.inspect.expression import used_fields
from dl_query_processing.compilation.primitives import (
    CompiledFormulaInfo,
    CompiledJoinOnFormulaInfo,
    CompiledOrderByFormulaInfo,
    CompiledQuery,
    FromColumn,
    JoinedFromObject,
    SubqueryFromObject,
)
from dl_query_processing.compilation.query_meta import QueryMetaInfo
from dl_query_processing.legacy_pipeline.separation.primitives import (
    CompiledLevel,
    CompiledMultiLevelQuery,
)
from dl_query_processing.legacy_pipeline.slicing.primitives import (
    SlicedFormula,
    SlicedJoinOnFormula,
    SlicedOrderByFormula,
    SlicedQuery,
)

LOGGER = logging.getLogger(__name__)

_COMPILED_FORMULA_TV = TypeVar("_COMPILED_FORMULA_TV", bound=CompiledFormulaInfo)


@attr.s
class QuerySeparator:
    _verbose_logging: bool = attr.ib(kw_only=True, default=False)

    def _log_info(self, *args, **kwargs) -> None:  # type: ignore  # TODO: fix
        if self._verbose_logging:
            LOGGER.info(*args, **kwargs)

    def _make_query_id(
        self,
        top_id: AvatarId,
        level_idx: int,
        query_idx: int,
        is_top_level: bool,
        iteration_id: str = "",
    ) -> AvatarId:
        # This will be used in the query.
        # So, to make it cacheable, the ID always has to be the same.
        if is_top_level:
            # Top-level ID must be preserved
            return top_id
        return f"{top_id}_{iteration_id}_{level_idx}_{query_idx}"

    def _restore_original_order_for_top_level_expressions(
        self,
        *,
        part_for_logging: str,
        top_level_expression_dict: Dict[str, _COMPILED_FORMULA_TV],
        sliced_expression_list: List[SlicedFormula],
    ) -> List[_COMPILED_FORMULA_TV]:
        reordered_top_level_expression_list: List[_COMPILED_FORMULA_TV] = []
        used_aliases: Set[str] = set()
        for sliced_formula in sliced_expression_list:
            # Get the top-level part
            top_level_parts_in_sliced_formula = sliced_formula.slices[-1].aliased_formulas
            assert (
                len(top_level_parts_in_sliced_formula) == 1
            ), "Every top-level formula slice must consist of a single formula"
            alias = next(iter(top_level_parts_in_sliced_formula.values())).alias
            top_level_formula = top_level_expression_dict[alias]  # type: ignore  # TODO: fix

            # Patch alias if it is a duplicate column
            patch_idx = 0
            while alias in used_aliases:
                alias = f"{alias}_cp{patch_idx}"
                top_level_formula = top_level_formula.clone(alias=alias)
                patch_idx += 1

            reordered_top_level_expression_list.append(top_level_formula)
            used_aliases.add(alias)  # type: ignore  # TODO: fix

        return reordered_top_level_expression_list

    def separate_query(self, sliced_query: SlicedQuery, iteration_id: str) -> CompiledMultiLevelQuery:
        # TODO: support multi-query levels

        query_top_level_idx = sliced_query.level_plan.level_count() - 1
        avatar_id_by_column: Dict[str, AvatarId] = {}
        formula_remapping_cache: NodeValueMap[Set[AvatarId]] = NodeValueMap()

        def remap_avatar_ids_in_formula(
            formula_piece: _COMPILED_FORMULA_TV,
            query_used_avatar_ids: Set[AvatarId],
        ) -> _COMPILED_FORMULA_TV:
            """Remap avatar ids in formula and register old and new contents of avatar_ids"""
            if level_idx != 0:  # 0-level formula do not require remapping because they use avatars directly
                # Otherwise use remapped ones from the previous level
                # First check the cache
                formula_obj = formula_piece.formula_obj
                avatar_ids = formula_remapping_cache.get(formula_obj)
                if avatar_ids is None:  # No hit, so do it and save the result to cache
                    avatar_ids = {avatar_id_by_column[f.name] for f in used_fields(formula_obj)}
                    formula_remapping_cache.add(formula_obj, value=avatar_ids)
                assert avatar_ids is not None
                formula_piece = formula_piece.clone(avatar_ids=avatar_ids)

            query_used_avatar_ids.update(formula_piece.avatar_ids)
            return formula_piece

        levels: List[CompiledLevel] = []
        for level_idx, level_type in enumerate(sliced_query.level_plan.level_types):
            is_top_level = level_idx == query_top_level_idx

            query_idx = 0  # query index in level

            query_id = self._make_query_id(
                top_id=sliced_query.id,
                level_idx=level_idx,
                query_idx=query_idx,
                is_top_level=is_top_level,
                iteration_id=iteration_id,
            )

            updated_query_used_avatar_ids: Set[AvatarId] = set()

            select_as_dict: Dict[str, CompiledFormulaInfo] = {}
            group_by_as_dict: Dict[str, CompiledFormulaInfo] = {}
            order_by_as_dict: Dict[str, CompiledOrderByFormulaInfo] = {}
            filters_as_dict: Dict[str, CompiledFormulaInfo] = {}
            join_on_as_dict: Dict[str, CompiledJoinOnFormulaInfo] = {}

            for sliced_formula_list, original_dest_dict in (
                (sliced_query.select, select_as_dict),
                (sliced_query.group_by, group_by_as_dict),
                (sliced_query.order_by, order_by_as_dict),
                (sliced_query.filters, filters_as_dict),
                (sliced_query.join_on, join_on_as_dict),
            ):
                for sliced_formula in sliced_formula_list:  # type: ignore  # TODO: fix
                    formula_top_level_idx = sliced_formula.level_plan.level_count() - 1
                    dest_dict = original_dest_dict
                    if level_idx > formula_top_level_idx:
                        # There are no parts of the formula at this level
                        continue
                    if level_idx < formula_top_level_idx:
                        # We're below the sliced_formula's execution level,
                        # so it goes into the SELECT part of the query
                        dest_dict = select_as_dict

                    current_slice = sliced_formula.slices[level_idx]

                    # Regenerate `avatar_ids` attributes for each formula piece
                    # and add all of these to the query's set of used avatars (updated_query_used_avatar_ids)
                    remapped_formula_dict = {
                        alias: remap_avatar_ids_in_formula(
                            formula_piece=formula_piece,
                            query_used_avatar_ids=updated_query_used_avatar_ids,
                        )
                        for alias, formula_piece in current_slice.aliased_formulas.items()
                    }

                    if dest_dict is order_by_as_dict:
                        # Special handling for ORDER BY
                        assert isinstance(sliced_formula, SlicedOrderByFormula)
                        dest_dict.update(  # type: ignore  # TODO: fix
                            {
                                alias: attrs_evolve_to_subclass(
                                    cls=CompiledOrderByFormulaInfo,
                                    inst=formula_piece,
                                    direction=sliced_formula.direction,
                                )
                                for alias, formula_piece in remapped_formula_dict.items()
                            }
                        )
                    elif dest_dict is join_on_as_dict:
                        # Special handling for JOIN ON
                        assert isinstance(sliced_formula, SlicedJoinOnFormula)
                        dest_dict.update(  # type: ignore  # TODO: fix
                            {
                                alias: attrs_evolve_to_subclass(
                                    cls=CompiledJoinOnFormulaInfo,
                                    inst=formula_piece,
                                    left_id=sliced_formula.left_id,
                                    right_id=sliced_formula.right_id,
                                    join_type=sliced_formula.join_type,
                                )
                                for alias, formula_piece in remapped_formula_dict.items()
                            }
                        )
                    else:
                        # All the rest
                        dest_dict.update(remapped_formula_dict)  # type: ignore  # TODO: fix
                        if dest_dict is select_as_dict:
                            # All columns in SELECT must be registered for remapping on higher levels
                            for alias in remapped_formula_dict:
                                if alias not in avatar_id_by_column:
                                    avatar_id_by_column[alias] = query_id

            self._log_info(
                f"{level_idx}-level query # {query_idx} "
                f"has ID {query_id} , level type {level_type.name} "
                f"and uses avatar IDs: {updated_query_used_avatar_ids}"
            )

            row_count_hard_limit: int
            field_order: Optional[list[tuple[int, str]]]

            if is_top_level:
                limit, offset = sliced_query.limit, sliced_query.offset
                # Top-level SELECTs and ORDER BYs must have the original order of expressions preserved
                select = self._restore_original_order_for_top_level_expressions(
                    part_for_logging="select",
                    sliced_expression_list=sliced_query.select,
                    top_level_expression_dict=select_as_dict,
                )
                order_by = self._restore_original_order_for_top_level_expressions(
                    part_for_logging="order_by",
                    sliced_expression_list=sliced_query.order_by,  # type: ignore  # TODO: fix
                    top_level_expression_dict=order_by_as_dict,
                )
                row_count_hard_limit = sliced_query.meta.row_count_hard_limit
                field_order = sliced_query.meta.field_order

            else:  # Non-top-level queries
                limit, offset = None, None
                select = [formula_piece for alias, formula_piece in sorted(select_as_dict.items())]
                order_by = [formula_piece for alias, formula_piece in sorted(order_by_as_dict.items())]
                row_count_hard_limit = DataAPILimits.DEFAULT_SOURCE_DB_LIMIT
                field_order = None

            if level_idx == 0:
                joined_from = sliced_query.joined_from
            else:
                previous_query = levels[-1].queries[0]
                joined_from = JoinedFromObject(
                    root_from_id=previous_query.id,
                    froms=[
                        SubqueryFromObject(
                            id=previous_query.id,
                            query_id=previous_query.id,
                            alias=previous_query.id,
                            columns=tuple(
                                FromColumn(id=select_formula.not_none_alias, name=select_formula.not_none_alias)
                                for select_formula in previous_query.select
                            ),
                        ),
                    ],
                )

            group_by = [formula_piece for alias, formula_piece in sorted(group_by_as_dict.items())]
            filters = [formula_piece for alias, formula_piece in sorted(filters_as_dict.items())]
            join_on = [formula_piece for alias, formula_piece in sorted(join_on_as_dict.items())]

            meta = QueryMetaInfo(
                row_count_hard_limit=row_count_hard_limit,
                query_type=sliced_query.meta.query_type,
                field_order=field_order,
                from_subquery=sliced_query.meta.from_subquery,  # FIXME: Only for the bottommost query
                subquery_limit=sliced_query.meta.subquery_limit,
                empty_query_mode=sliced_query.meta.empty_query_mode,
            )

            ls_query = CompiledQuery(
                id=query_id,
                level_type=level_type,
                select=select,
                group_by=group_by,
                order_by=order_by,
                filters=filters,
                join_on=join_on,
                joined_from=joined_from,
                limit=limit,
                offset=offset,
                meta=meta,
            )
            level = CompiledLevel(level_type=level_type, queries=[ls_query])
            levels.append(level)

        assert len(levels[query_top_level_idx].queries) == 1, "There must be only one top level query"

        multi_level_query = CompiledMultiLevelQuery(
            levels=levels,
        )
        return multi_level_query
