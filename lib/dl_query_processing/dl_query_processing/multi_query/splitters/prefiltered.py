from __future__ import annotations

from typing import (
    Any,
    ClassVar,
    Optional,
)

import attr

import dl_formula.core.nodes as formula_nodes
from dl_query_processing.compilation.primitives import (
    CompiledFormulaInfo,
    CompiledJoinOnFormulaInfo,
    CompiledMultiQuery,
    CompiledMultiQueryBase,
    CompiledQuery,
    FromColumn,
    FromObject,
    JoinedFromObject,
    SubqueryFromObject,
)
from dl_query_processing.compilation.query_meta import QueryMetaInfo
from dl_query_processing.enums import (
    ExecutionLevel,
    QueryPart,
)
from dl_query_processing.multi_query.splitters.base import MultiQuerySplitterBase
from dl_query_processing.multi_query.tools import CompiledMultiQueryPatch
from dl_query_processing.utils.name_gen import PrefixedIdGen


@attr.s
class PrefilteredFieldMultiQuerySplitter(MultiQuerySplitterBase):
    """
    Splits off sub-queries at field level.
    """

    duplicate_prefilters: ClassVar[bool] = True

    crop_to_level_type: ExecutionLevel = attr.ib(kw_only=True)

    def is_pre_filter(self, formula: CompiledFormulaInfo) -> bool:
        """By default pre-filters are not supported"""
        return False

    def is_simple_query(self, query: CompiledQuery) -> bool:
        if query.group_by or query.order_by or query.join_on:
            return False
        if not all(self.is_pre_filter(formula) for formula in query.filters):
            return False
        for formula in query.select:
            # Check whether formula is a simple field expression
            if not isinstance(formula.formula_obj.expr, formula_nodes.Field):
                return False

        return True

    def split_query(
        self,
        query: CompiledQuery,
        requirement_subtree: CompiledMultiQueryBase,
        query_id_gen: PrefixedIdGen,
        expr_id_gen: PrefixedIdGen,
    ) -> Optional[CompiledMultiQueryPatch]:
        # First check if we really need to do anything
        if self.is_simple_query(query):
            return None

        field_to_new_alias_map: dict[str, str] = {}
        avatar_subq_map: dict[str, str] = {}

        old_fields_to_from_ids: dict[str, str] = {}
        used_fields_by_avatar: dict[str, list[str]] = {}
        for original_from in query.joined_from.froms:
            original_from_id = original_from.id
            new_from_id = query_id_gen.get_id()
            avatar_subq_map[original_from_id] = new_from_id
            used_fields_by_avatar[original_from_id] = []
            for col in original_from.columns:
                old_fields_to_from_ids[col.id] = original_from_id

        def match_func(node: formula_nodes.FormulaItem, *args: Any) -> bool:
            return isinstance(node, formula_nodes.Field)

        def replace_func(node: formula_nodes.FormulaItem, *args: Any) -> formula_nodes.FormulaItem:
            assert isinstance(node, formula_nodes.Field)
            original_field_name = node.name
            new_field_name = field_to_new_alias_map.get(original_field_name)
            if new_field_name is None:
                new_field_name = expr_id_gen.get_id()
                field_to_new_alias_map[original_field_name] = new_field_name
                original_from_id = old_fields_to_from_ids[original_field_name]
                used_fields_by_avatar[original_from_id].append(original_field_name)
            assert new_field_name is not None
            return formula_nodes.Field.make(name=new_field_name, meta=node.meta)

        query_parts = (
            QueryPart.select,
            QueryPart.group_by,
            QueryPart.filters,
            QueryPart.order_by,
            QueryPart.join_on,
        )

        cropped_formula_list_by_query_part: dict[QueryPart, list[CompiledFormulaInfo]] = {}
        prefilters_by_from_id: dict[str, list[CompiledFormulaInfo]] = {}
        for query_part in query_parts:
            formula_list = query.get_formula_list(query_part)
            cropped_formula_list: list[CompiledFormulaInfo] = []
            for formula in formula_list:
                if self.is_pre_filter(formula):
                    # Send pre-filters to the sub-queries
                    assert len(formula.avatar_ids) == 1
                    from_id = next(iter(formula.avatar_ids))
                    if from_id not in prefilters_by_from_id:
                        prefilters_by_from_id[from_id] = []
                    prefilters_by_from_id[from_id].append(formula)

                    if not self.duplicate_prefilters:
                        # Pre-filter duplication is off, so do not add the filter to the cropped query
                        continue

                formula_obj = formula.formula_obj
                # Replace all field names with sub-query-referencing aliases
                cropped_formula_obj = formula_obj.replace_nodes(match_func=match_func, replace_func=replace_func)
                used_subquery_ids = {avatar_subq_map[from_id] for from_id in formula.avatar_ids}
                formula = formula.clone(
                    formula_obj=cropped_formula_obj,
                    avatar_ids=used_subquery_ids,
                )
                if isinstance(formula, CompiledJoinOnFormulaInfo):
                    formula = formula.clone(
                        left_id=avatar_subq_map[formula.left_id],
                        right_id=avatar_subq_map[formula.right_id],
                    )
                cropped_formula_list.append(formula)

            cropped_formula_list_by_query_part[query_part] = cropped_formula_list

        subqueries: list[CompiledQuery] = []

        cropped_froms: list[FromObject] = []
        for original_from in query.joined_from.froms:
            original_from_id = original_from.id
            new_from_id = avatar_subq_map[original_from_id]

            columns: list[FromColumn] = []
            for col in original_from.columns:
                new_col_id = field_to_new_alias_map.get(col.id)
                if new_col_id is None:
                    continue  # This column is not used in the query, so skip it
                columns.append(FromColumn(id=new_col_id, name=new_col_id))

            cropped_froms.append(
                SubqueryFromObject(
                    id=new_from_id,
                    alias=new_from_id,
                    query_id=new_from_id,
                    columns=tuple(columns),
                )
            )

            subquery_select = [
                CompiledFormulaInfo(
                    formula_obj=formula_nodes.Formula.make(expr=formula_nodes.Field.make(name=field_name)),
                    alias=field_to_new_alias_map[field_name],
                    avatar_ids={original_from_id},
                    original_field_id=None,
                )
                for field_name in used_fields_by_avatar[original_from_id]
            ]

            subquery_filters: list[CompiledFormulaInfo] = []
            if original_from_id in prefilters_by_from_id:
                for prefilter_formula in prefilters_by_from_id[original_from_id]:
                    subquery_filters.append(prefilter_formula)

            subquery = CompiledQuery(
                id=new_from_id,
                select=subquery_select,
                filters=subquery_filters,
                level_type=query.level_type,
                joined_from=JoinedFromObject(
                    root_from_id=original_from_id,
                    froms=[original_from],
                ),
                meta=QueryMetaInfo(
                    query_type=query.meta.query_type,
                    field_order=None,
                    from_subquery=query.meta.from_subquery,
                    subquery_limit=query.meta.subquery_limit,
                    empty_query_mode=query.meta.empty_query_mode,
                    row_count_hard_limit=query.meta.row_count_hard_limit,
                ),
            )
            subqueries.append(subquery)

        new_root_from_id: Optional[str] = None
        if query.joined_from.root_from_id is not None:
            new_root_from_id = avatar_subq_map[query.joined_from.root_from_id]

        cropped_joined_from = JoinedFromObject(root_from_id=new_root_from_id, froms=cropped_froms)
        cropped_query = query.clone(
            select=cropped_formula_list_by_query_part[QueryPart.select],
            group_by=cropped_formula_list_by_query_part[QueryPart.group_by],
            filters=cropped_formula_list_by_query_part[QueryPart.filters],
            order_by=cropped_formula_list_by_query_part[QueryPart.order_by],
            join_on=cropped_formula_list_by_query_part[QueryPart.join_on],
            joined_from=cropped_joined_from,
            level_type=self.crop_to_level_type,
        )

        patch = CompiledMultiQueryPatch(patch_multi_query=CompiledMultiQuery(queries=[cropped_query, *subqueries]))
        return patch
