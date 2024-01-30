from __future__ import annotations

import abc
from enum import Enum
import itertools
from typing import (
    Any,
    ClassVar,
    Collection,
    Optional,
    Sequence,
)

import attr

from dl_constants.enums import JoinType
import dl_formula.core.aux_nodes as formula_aux_nodes
import dl_formula.core.exc as formula_exc
from dl_formula.core.extract import NodeExtract
import dl_formula.core.fork_nodes as formula_fork_nodes
from dl_formula.core.index import NodeHierarchyIndex
import dl_formula.core.nodes as formula_nodes
import dl_formula.inspect.expression as inspect_expression
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
from dl_query_processing.enums import QueryPart
from dl_query_processing.multi_query.splitters.base import MultiQuerySplitterBase
from dl_query_processing.multi_query.tools import (
    CompiledMultiQueryPatch,
    remap_formula_obj_fields,
)
from dl_query_processing.utils.name_gen import PrefixedIdGen


class SubqueryType(Enum):
    window_func = "window_func"
    default = "default"
    generated_base = "generated_base"


@attr.s(frozen=True)
class FormulaSplitMask:
    """
    Base class for `AliasedFormulaSplitMask` without the `alias` field;
    not intended for widespread usage.
    See `AliasedFormulaSplitMask` for more info.
    """

    # Indicates which part (clause) of the original query the formula is an item of
    query_part: QueryPart = attr.ib(kw_only=True)

    # The index within list specified by `query_part`
    formula_list_idx: int = attr.ib(kw_only=True)

    # Specifies the node which should be cut out from the original query
    # and substituted for a subquery column
    outer_node_idx: NodeHierarchyIndex = attr.ib(kw_only=True)

    # Specifies the node which should be used in the new sub-query.
    # Can be the same as outer_*, but can point to its sub-node
    # If some auxiliary nodes in-between need to be removed
    # from the queries completely.
    inner_node_idx: NodeHierarchyIndex = attr.ib(kw_only=True)

    def __attrs_post_init__(self) -> None:
        assert self.inner_node_idx.startswith(self.outer_node_idx)


@attr.s(frozen=True)
class AliasedFormulaSplitMask(FormulaSplitMask):
    """
    Formula-splitting mask.
    Performs two functions:

    1. Is used to generate SELECT items for sub-queries being split off the main query.
       `query_part`, `formula_list_idx` and `inner_node_idx` indicate where to find the
       expression in the original query, while `alias` tells us how to label the new
       split-off formula.

    2. Is used to "cut out" an expression from the original query and replace it with
       a `Field` node that now refers to a subquery where the cut-out part
       of the original expression went.
       `query_part`, `formula_list_idx` and `outer_node_idx` indicate what to cut,
       and `alias` is the name of the new `Field` node.
    """

    alias: str = attr.ib(kw_only=True)


@attr.s(frozen=True)
class AddFormulaInfo:
    """
    Is used to specify a formula that is being added to a sub-query.
    Used for dimensions and for measures that are being added to be used in
    JOIN ON expressions.
    """

    alias: str = attr.ib(kw_only=True)
    expr: formula_nodes.FormulaItem = attr.ib(kw_only=True)
    from_ids: frozenset[str] = attr.ib(kw_only=True)
    is_group_by: bool = attr.ib(kw_only=True)

    def clone(self, **kwargs: Any) -> AddFormulaInfo:
        return attr.evolve(self, **kwargs)


@attr.s(frozen=True)
class QuerySplitMask:
    subquery_type: SubqueryType = attr.ib(kw_only=True)
    subquery_id: str = attr.ib(kw_only=True)
    formula_split_masks: tuple[AliasedFormulaSplitMask, ...] = attr.ib(kw_only=True)
    add_formulas: tuple[AddFormulaInfo, ...] = attr.ib(kw_only=True)
    filter_indices: frozenset[int] = attr.ib(kw_only=True)
    join_type: Optional[JoinType] = attr.ib(kw_only=True)
    joining_node: Optional[formula_fork_nodes.QueryForkJoiningBase] = attr.ib(kw_only=True)
    is_base: bool = attr.ib(kw_only=True, default=False)

    # calculated props
    group_by_count: int = attr.ib(init=False)

    @group_by_count.default
    def _make_group_by_count(self) -> int:
        non_const_cnt = 0
        for dim in self.add_formulas:
            if dim.is_group_by:
                non_const_cnt += 1
        return non_const_cnt

    @property
    def has_direct_equality_join(self) -> bool:
        """
        True if the join contains only simple equalities
        of column from one sub-query to the corresponding column from the other sub-query.
        """

        assert isinstance(self.joining_node, formula_fork_nodes.QueryForkJoiningWithList)
        return all(
            isinstance(condition, formula_fork_nodes.SelfEqualityJoinCondition)
            for condition in self.joining_node.condition_list
        )

    def clone(self, **kwargs: Any) -> QuerySplitMask:
        return attr.evolve(self, **kwargs)


@attr.s
class MultiQuerySplitter(MultiQuerySplitterBase):
    parent_query_parts_for_splitting: ClassVar[tuple[QueryPart, ...]] = (
        QueryPart.select,
        QueryPart.order_by,
        QueryPart.filters,
    )

    @abc.abstractmethod
    def get_split_masks(
        self,
        query: CompiledQuery,
        expr_id_gen: PrefixedIdGen,
        query_id_gen: PrefixedIdGen,
    ) -> list[QuerySplitMask]:
        raise NotImplementedError

    def mutate_split_node(self, node: formula_nodes.FormulaItem) -> formula_nodes.FormulaItem:
        """
        Optionally mutate the node that was split off by the mask.
        By default, return it without any changes.
        """

        return node

    def mutate_cropped_query(self, query: CompiledQuery) -> CompiledQuery:
        """
        Optionally mutate the original query after subqueries have been split off it.
        """

        return query

    def _get_field_to_from_id_map(
        self,
        requirement_subtree: CompiledMultiQueryBase,
    ) -> dict[str, str]:
        result: dict[str, str] = {}
        for top_query in requirement_subtree.get_top_queries():
            for select_formula in top_query.select:
                result[select_formula.not_none_alias] = top_query.id

        return result

    def _generate_subquery_for_mask(
        self,
        query: CompiledQuery,
        split_mask: QuerySplitMask,
    ) -> CompiledQuery:
        """
        Generate sub-query described by a `QuerySplitMask`.
        """

        select: list[CompiledFormulaInfo] = []
        group_by: list[CompiledFormulaInfo] = []
        select_by_alias: dict[str, CompiledFormulaInfo] = {}
        for formula_split_mask in split_mask.formula_split_masks:
            formula_list = query.get_formula_list(formula_split_mask.query_part)
            original_formula = formula_list[formula_split_mask.formula_list_idx]
            alias = formula_split_mask.alias
            # Make sure the index doesn't point to the top-level Formula object
            assert len(formula_split_mask.inner_node_idx.indices) > 0

            split_off_formula_obj = original_formula.formula_obj[formula_split_mask.inner_node_idx]
            split_off_formula_obj = self.mutate_split_node(split_off_formula_obj)
            new_formula_obj = formula_nodes.Formula.make(expr=split_off_formula_obj)

            avatar_ids: set[str] = original_formula.avatar_ids
            select_formula = CompiledFormulaInfo(
                formula_obj=new_formula_obj,
                alias=alias,
                original_field_id=original_formula.original_field_id,
                avatar_ids=avatar_ids,
            )

            # Deduplicate formulas that are the same
            existing_formula = select_by_alias.get(alias)
            if existing_formula is not None:
                # Already added a formula for this alias. Make sure they are identical.
                assert (
                    select_formula.formula_obj == existing_formula.formula_obj
                ), "Got different SELECT formulas for the same alias"

            else:
                # This formula has not been added yet.
                select.append(select_formula)
                select_by_alias[alias] = select_formula

        for add_formula in split_mask.add_formulas:
            formula = CompiledFormulaInfo(
                formula_obj=formula_nodes.Formula.make(expr=add_formula.expr),
                alias=add_formula.alias,
                original_field_id=None,
                avatar_ids=set(add_formula.from_ids),
            )
            select.append(formula)
            if add_formula.is_group_by:
                group_by.append(formula)

        filters: list[CompiledFormulaInfo] = [
            formula for filter_idx, formula in enumerate(query.filters) if filter_idx in split_mask.filter_indices
        ]

        join_on = query.join_on
        joined_from = query.joined_from

        subquery = CompiledQuery(
            id=split_mask.subquery_id,
            level_type=query.level_type,
            select=select,
            group_by=group_by,
            order_by=[],  # ORDER BY only in the top-level sub-query
            filters=filters,
            join_on=join_on,
            joined_from=joined_from,
            meta=QueryMetaInfo(
                query_type=query.meta.query_type,
                field_order=None,
                from_subquery=query.meta.from_subquery,
                subquery_limit=query.meta.subquery_limit,
                empty_query_mode=query.meta.empty_query_mode,
                row_count_hard_limit=query.meta.row_count_hard_limit,
            ),
        )
        return subquery

    def _make_join_on_expression(
        self,
        left_subquery_mask: QuerySplitMask,
        right_subquery_mask: QuerySplitMask,
    ) -> Optional[CompiledJoinOnFormulaInfo]:
        def and_part(condition: Optional[formula_nodes.Binary], part: formula_nodes.Binary) -> formula_nodes.Binary:
            if condition is None:
                return part
            return formula_nodes.Binary.make(name="and", left=condition, right=part)

        join_expr: Optional[formula_nodes.Binary] = None

        aliases_from_right_to_left: dict[str, str] = {}
        left_map = {add_formula.expr.extract: add_formula.alias for add_formula in left_subquery_mask.add_formulas}
        right_map = {add_formula.expr.extract: add_formula.alias for add_formula in right_subquery_mask.add_formulas}
        for node_extract, right_alias in right_map.items():
            if node_extract in left_map:
                aliases_from_right_to_left[right_alias] = left_map[node_extract]

        if isinstance(right_subquery_mask.joining_node, formula_fork_nodes.QueryForkJoiningWithList):
            # Joining node explicitly lists joining conditions.
            for condition in right_subquery_mask.joining_node.condition_list:
                right_expr: formula_nodes.FormulaItem
                left_expr: formula_nodes.FormulaItem
                if isinstance(condition, formula_fork_nodes.SelfEqualityJoinCondition):
                    expr = condition.expr
                    if not isinstance(expr, (formula_nodes.Field, formula_aux_nodes.ErrorNode)):
                        raise TypeError(f"Expected field or error node, got {type(expr)}")

                    left_expr = expr
                    right_expr = expr
                elif isinstance(condition, formula_fork_nodes.BinaryJoinCondition):
                    left_expr = condition.expr
                    right_expr = condition.fork_expr
                else:
                    raise TypeError(f"Type {type(condition).__name__} is not supported")

                # Remap columns in left_expr
                # because it has to be evaluated against the left sub-query,
                # and not the right one, whose field names are used in the expression
                left_expr = remap_formula_obj_fields(node=left_expr, field_name_map=aliases_from_right_to_left)
                part = formula_nodes.Binary.make(name="_dneq", left=left_expr, right=right_expr)
                join_expr = and_part(condition=join_expr, part=part)

        else:
            raise TypeError(f"Joining node type {type(right_subquery_mask.joining_node).__name__} is not supported")

        if join_expr is None:
            return None

        join_type = right_subquery_mask.join_type
        assert join_type is not None

        return CompiledJoinOnFormulaInfo(
            alias=None,  # Will not be used
            formula_obj=formula_nodes.Formula.make(expr=join_expr),
            avatar_ids={left_subquery_mask.subquery_id, right_subquery_mask.subquery_id},
            original_field_id=None,
            left_id=left_subquery_mask.subquery_id,
            right_id=right_subquery_mask.subquery_id,
            join_type=join_type,
        )

    def get_used_from_ids(
        self,
        formula_obj: formula_nodes.Formula,
        formula_alias_to_subquery_id_map: dict[str, str],
    ) -> set[str]:
        used_fields = inspect_expression.used_fields(formula_obj)
        avatar_ids: set[str] = {formula_alias_to_subquery_id_map[field_node.name] for field_node in used_fields}
        return avatar_ids

    def _replace_dimensions_with_subquery_fields(
        self,
        formula_obj: formula_nodes.Formula,
        base_gb_alias_by_extract: dict[NodeExtract, str],
    ) -> formula_nodes.Formula:
        """
        Replace all dimensions in a formula with field nodes
        referencing these dimensions in the sub-query.
        """

        def match_func(node: formula_nodes.FormulaItem, *_: Any) -> bool:
            return node.extract_not_none in base_gb_alias_by_extract

        def replace_func(node: formula_nodes.FormulaItem, *_: Any) -> formula_nodes.FormulaItem:
            dim_field_name = base_gb_alias_by_extract[node.extract_not_none]
            return formula_nodes.Field.make(name=dim_field_name, meta=node.meta)

        return formula_obj.replace_nodes(match_func=match_func, replace_func=replace_func)

    def _generate_cropped_formula_list(
        self,
        query: CompiledQuery,
        query_part: QueryPart,
        gb_aliases: set[str],
        formula_split_masks: list[AliasedFormulaSplitMask],
        base_gb_alias_by_extract: dict[NodeExtract, str],
        formula_alias_to_subquery_id_map: dict[str, str],
        substitute_error_node: Optional[formula_aux_nodes.ErrorNode] = None,
        exclude_indices: Collection[int] = frozenset(),
    ) -> list[CompiledFormulaInfo]:
        if exclude_indices:
            assert query_part == QueryPart.filters, "Only filter formulas can be excluded during query cropping"

        formula_list = query.get_formula_list(query_part)

        formula_masks_by_list_idx: dict[int, list[AliasedFormulaSplitMask]] = {}
        for formula_split_mask in formula_split_masks:
            if formula_split_mask.query_part != query_part:
                continue

            if formula_split_mask.formula_list_idx not in formula_masks_by_list_idx:
                formula_masks_by_list_idx[formula_split_mask.formula_list_idx] = []

            formula_masks_by_list_idx[formula_split_mask.formula_list_idx].append(formula_split_mask)

        new_formula_list: list[CompiledFormulaInfo] = []
        for formula_idx, formula in enumerate(formula_list):
            if formula_idx in exclude_indices:
                # Skip formula. Only for filters (see check above)
                continue

            formula_obj = formula.formula_obj
            is_constant = inspect_expression.is_constant_expression(formula_obj.expr)

            if is_constant:
                # Constant dimensions remain unchanged
                pass

            # Separate logic for expressions that are dimensions
            elif formula.not_none_alias in gb_aliases:
                dim_field_name = base_gb_alias_by_extract[formula_obj.expr.extract_not_none]
                formula_obj = formula_nodes.Formula.make(
                    expr=formula_nodes.Field.make(
                        name=dim_field_name,
                        meta=formula_obj.expr.meta,
                    ),
                    meta=formula_obj.meta,
                )

            # A compatibility error has to be thrown, so put that instead of the field reference
            elif substitute_error_node is not None:
                formula_obj = formula_nodes.Formula.make(expr=substitute_error_node)

            # And for other fields (measures or dimension-dependent filters)
            # substitute expressions for field references to sub-query
            else:
                for formula_split_mask in formula_masks_by_list_idx.get(formula_idx, ()):
                    formula_obj = formula_obj.replace_at_index(
                        index=formula_split_mask.outer_node_idx,
                        expr=formula_nodes.Field.make(
                            name=formula_split_mask.alias,
                            meta=formula_obj.expr.meta,
                        ),
                    )
                if query_part == QueryPart.filters:
                    # Only filters can depend on dimensions, but not be dimensions themselves
                    formula_obj = self._replace_dimensions_with_subquery_fields(
                        formula_obj=formula_obj,
                        base_gb_alias_by_extract=base_gb_alias_by_extract,
                    )

            has_been_updated = formula_obj is not formula.formula_obj
            if has_been_updated:
                formula = formula.clone(
                    formula_obj=formula_obj,
                    avatar_ids=self.get_used_from_ids(formula_obj, formula_alias_to_subquery_id_map),
                )

            elif not is_constant:
                # Can't add a formula that has not been split
                # (it references the original FROM (avatar/sub-query)
                # that should not be accessible from this sub-query).
                # In theory this should happen only to filters.
                # Filters might need to be excluded if all non-dimensions
                # have BFB for this filter.
                assert query_part is QueryPart.filters
                continue

            new_formula_list.append(formula)

        return new_formula_list

    def _are_subquery_dimensions_compatible(self, split_masks: Collection[QuerySplitMask]) -> bool:
        if not split_masks:
            return True

        dimension_sets = [
            frozenset(dim.expr.extract for dim in mask.add_formulas if dim.is_group_by) for mask in split_masks
        ]
        max_dim_set = frozenset(itertools.chain.from_iterable(dimension_sets))
        return max_dim_set in dimension_sets

    def _crop_original_query(
        self,
        query: CompiledQuery,
        split_masks: list[QuerySplitMask],
        subqueries_compatible: bool,
    ) -> CompiledQuery:
        # Combine split indices from all subqueries into one list
        formula_split_masks: list[AliasedFormulaSplitMask] = []
        for subquery_mask in split_masks:
            formula_split_masks += subquery_mask.formula_split_masks

        base_subquery_mask, other_subquery_masks = self._separate_base_and_other_masks(split_masks)
        base_gb_alias_by_extract = {
            dim.expr.extract_not_none: dim.alias for dim in base_subquery_mask.add_formulas if dim.is_group_by
        }

        # Build map {<formula alias>: <subquery id>}
        # from 1) formula split masks, 2) additional formulas
        formula_alias_to_subquery_id_map: dict[str, str] = {}
        for subquery_mask in split_masks:
            for formula_split_mask in subquery_mask.formula_split_masks:
                formula_alias_to_subquery_id_map[formula_split_mask.alias] = subquery_mask.subquery_id
            for add_formula in subquery_mask.add_formulas:
                formula_alias_to_subquery_id_map[add_formula.alias] = subquery_mask.subquery_id

        substitute_error_node: Optional[formula_aux_nodes.ErrorNode] = None
        if not subqueries_compatible:
            substitute_error_node = formula_aux_nodes.ErrorNode.make(
                message="LOD dimensions are incompatible",
                err_code=formula_exc.LodIncompatibleDimensionsError.default_code,
            )

        gb_aliases: set[str] = set()  # Aliases of GROUP BY expressions
        for formula in query.group_by:
            gb_aliases.add(formula.not_none_alias)
            formula_obj_expr = formula.formula_obj.expr
            # Validate that the base mask has this expression among its dimensions
            if not inspect_expression.is_constant_expression(formula_obj_expr):
                assert formula_obj_expr.extract_not_none in base_gb_alias_by_extract

        def generate_formula_list(
            query_part: QueryPart,
            exclude_indices: Collection[int] = frozenset(),
        ) -> list[CompiledFormulaInfo]:
            """A convenience wrapper for method"""
            return self._generate_cropped_formula_list(
                query=query,
                query_part=query_part,
                gb_aliases=gb_aliases,
                base_gb_alias_by_extract=base_gb_alias_by_extract,
                formula_alias_to_subquery_id_map=formula_alias_to_subquery_id_map,
                formula_split_masks=formula_split_masks,
                substitute_error_node=substitute_error_node,
                exclude_indices=exclude_indices,
            )

        # Apply replacements to the original query's formula lists
        new_select = generate_formula_list(query_part=QueryPart.select)
        new_group_by = generate_formula_list(query_part=QueryPart.group_by)
        new_order_by = generate_formula_list(query_part=QueryPart.order_by)

        # Remove filters that have already been applied a lower level.
        new_filters = generate_formula_list(
            query_part=QueryPart.filters, exclude_indices=base_subquery_mask.filter_indices
        )

        # Generate JOIN ON and joined FROM
        join_on: list[CompiledJoinOnFormulaInfo] = []
        froms: list[FromObject] = []
        for subquery_mask in split_masks:
            columns: list[FromColumn] = []
            added_col_aliases: set[str] = set()
            item: AddFormulaInfo | AliasedFormulaSplitMask
            for item in itertools.chain(subquery_mask.formula_split_masks, subquery_mask.add_formulas):  # type: ignore  # 2024-01-30 # TODO: Incompatible types in assignment (expression has type "object", variable has type "AddFormulaInfo | AliasedFormulaSplitMask")  [assignment]
                alias = item.alias
                if alias not in added_col_aliases:
                    columns.append(FromColumn(id=alias, name=alias))
                    added_col_aliases.add(alias)
            froms.append(
                SubqueryFromObject(
                    id=subquery_mask.subquery_id,
                    alias=subquery_mask.subquery_id,
                    query_id=subquery_mask.subquery_id,
                    columns=tuple(columns),
                )
            )
            if not subqueries_compatible:
                # Queries are incompatible, so no point in generating JOIN ON
                continue
            if subquery_mask is not base_subquery_mask:
                join_on_expr = self._make_join_on_expression(
                    left_subquery_mask=base_subquery_mask,
                    right_subquery_mask=subquery_mask,
                )
                if join_on_expr is not None:
                    join_on.append(join_on_expr)

        root_from_id = froms[0].id
        joined_from = JoinedFromObject(root_from_id=root_from_id, froms=froms)

        # Update the original query object
        query = query.clone(
            select=new_select,
            group_by=new_group_by,
            filters=new_filters,
            order_by=new_order_by,
            join_on=join_on,
            joined_from=joined_from,
        )
        query = self.mutate_cropped_query(query)
        return query

    def _get_formula_masks_as_dict(
        self,
        split_masks: Sequence[QuerySplitMask],
    ) -> dict[QueryPart, dict[int, set[NodeHierarchyIndex]]]:
        """
        reorganize all split-indices from `split_masks` into dict with the following structure:

        {
            <query_part>: {
                <list_index_in_query_part>: {<split_node_idx_1>, <spdsplit_node_idx_2>, ...}
            }
        }
        """

        formula_mask_dict: dict[QueryPart, dict[int, set[NodeHierarchyIndex]]] = {}
        for mask in split_masks:
            for formula_split_mask in mask.formula_split_masks:
                if formula_split_mask.query_part not in formula_mask_dict:
                    formula_mask_dict[formula_split_mask.query_part] = {}
                by_query_part = formula_mask_dict[formula_split_mask.query_part]
                if formula_split_mask.formula_list_idx not in by_query_part:
                    by_query_part[formula_split_mask.formula_list_idx] = set()
                by_query_part[formula_split_mask.formula_list_idx].add(formula_split_mask.outer_node_idx)

        return formula_mask_dict

    def _get_formula_mask_counterparts_for_formula(
        self,
        root_node: formula_nodes.Formula,
        incoming_masks: Sequence[NodeHierarchyIndex],
    ) -> list[NodeHierarchyIndex]:
        incoming_mask_idx = 0
        result: list[NodeHierarchyIndex] = []

        def collect_recursively(node: formula_nodes.FormulaItem, prefix: NodeHierarchyIndex) -> None:
            nonlocal incoming_mask_idx

            node_is_const = inspect_expression.is_constant_expression(node)

            while incoming_mask_idx < len(incoming_masks):
                mask = incoming_masks[incoming_mask_idx]
                if mask >= prefix:
                    break
                incoming_mask_idx += 1
            else:
                # `break` was never called;
                # there are no more masks.
                # So add this node since it doesn't match the mask
                if not node_is_const:  # ignore constants
                    result.append(prefix)
                # Nothing more to do here
                return

            # At this point `mask >= prefix`

            if prefix < mask:
                if not mask.startswith(prefix):
                    # Mask does not point to a child
                    if not node_is_const:  # ignore constants
                        result.append(prefix)
                else:
                    # Mask does point to a child node
                    autonomous_children = list(inspect_expression.enumerate_autonomous_children(node, prefix=prefix))
                    # `autonomous_children` is used instead of node.children because
                    # We don't want to iterate over auxiliary nodes like LOD, BFB, etc. here -
                    # anything that we can't split off.
                    if not autonomous_children:
                        # mask points to nonexistent children
                        raise ValueError("Invalid node index")

                    else:
                        for child_node_idx, child, _ in autonomous_children:
                            collect_recursively(child, child_node_idx)

            # If `prefix == mask`, then this node is blacklisted, skip it.

        collect_recursively(root_node.expr, prefix=NodeHierarchyIndex(indices=(0,)))
        return result

    def _get_formula_mask_counterparts(
        self,
        query: CompiledQuery,
        split_masks: Sequence[QuerySplitMask],
        expr_id_gen: PrefixedIdGen,
    ) -> list[AliasedFormulaSplitMask]:
        """
        Generate "counterpart" split-indices for the ones given.
        Basically what this means is that for every incoming split-index
        the result will include an index for each of its siblings (child nodes of the same parent node)
        not covered by other incoming indices. Ignore constants - don't generate split-indices for them.
        """

        formula_mask_dict = self._get_formula_masks_as_dict(split_masks=split_masks)

        counter_formula_split_masks: list[AliasedFormulaSplitMask] = []

        gb_aliases = [formula.not_none_alias for formula in query.group_by]  # Aliases of GROUP BY expressions

        for query_part in self.parent_query_parts_for_splitting:
            by_list_idx = formula_mask_dict.get(query_part, {})
            formula_list = query.get_formula_list(query_part)
            for formula_list_idx, formula in enumerate(formula_list):
                if formula.alias in gb_aliases:
                    # Do not split dimensions
                    continue

                if inspect_expression.is_constant_expression(formula.formula_obj.expr):
                    # Do not split constant expressions
                    continue

                node_index_list = by_list_idx.get(formula_list_idx, ())
                if query_part == QueryPart.filters and not node_index_list:
                    # If a filter has not yet been split, don't split it now
                    continue

                counter_formula_indices = self._get_formula_mask_counterparts_for_formula(
                    root_node=formula.formula_obj,
                    incoming_masks=sorted(node_index_list),
                )
                if counter_formula_indices:
                    for node_idx in counter_formula_indices:
                        counter_formula_split_masks.append(
                            AliasedFormulaSplitMask(
                                alias=expr_id_gen.get_id(),
                                query_part=query_part,
                                formula_list_idx=formula_list_idx,
                                inner_node_idx=node_idx,
                                outer_node_idx=node_idx,
                            )
                        )

        return counter_formula_split_masks

    def _find_base_query_candidate(
        self,
        query: CompiledQuery,
        split_masks: list[QuerySplitMask],
        base_formula_split_masks: list[AliasedFormulaSplitMask],
        base_filter_indices: set[int],
    ) -> Optional[str]:
        """
        Here we try to determine if there is a need to generate a "base" sub-query
        that all the explicitly generated ones will be joined to.

        If there is a sub-query that has the same dimensions and same filters
        as one of the generated sub-queries and has no other expressions
        that need to be selected, we can use this existing sub-query as the base.
        """

        if base_formula_split_masks:
            # There are parts of the original query's expressions
            # that need to be selected, but are not part of other sub-queries
            # In this case we will need to select them as part of the base sub-query
            return None

        base_group_by_extracts = {
            formula.formula_obj.expr.extract_not_none
            for formula in query.group_by
            if not inspect_expression.is_constant_expression(formula.formula_obj.expr)
        }

        # Count the number of dimensions the base sub-query needs to have
        base_group_by_count = len(base_group_by_extracts)

        # Try to find a candidate for being the base sub-query among the existing sub-query masks
        max_dimensions = max(mask.group_by_count for mask in split_masks)

        max_query_id: Optional[str] = None
        max_query_has_all_base_dimensions = False
        max_query_has_all_base_filters = True
        max_query_has_direct_join = True
        for mask in split_masks:
            if mask.group_by_count != max_dimensions:
                # We are looking for the sub-query with the most dimensions
                continue

            mask_group_by_extracts = {
                add_formula.expr.extract_not_none for add_formula in mask.add_formulas if add_formula.is_group_by
            }
            if not mask_group_by_extracts.issuperset(base_group_by_extracts):
                # The sub-query has to have all of the original query's dimensions
                continue

            # Otherwise this is the largest sub-query (by number of dimensions)
            max_query_id = mask.subquery_id
            max_query_has_all_base_dimensions = mask.group_by_count == base_group_by_count
            max_query_has_all_base_filters = set(mask.filter_indices) == base_filter_indices
            max_query_has_direct_join = mask.has_direct_equality_join

        if max_query_has_all_base_dimensions and max_query_has_all_base_filters and max_query_has_direct_join:
            # No selects left for the original query,
            # and subquery with the most dimensions has the same
            # dimensions and filters as would the base sub-query.
            # So we can just use this "largest" subquery as the base.
            assert max_query_id is not None
            return max_query_id

        if base_group_by_count == 0:
            # Base query doesn't have any own dimensions
            # (inherited non-const ones from the original query).
            # If there are supposed to be any filters,
            # they are useless in a 0-dimensional query with no selects.
            # So don't create a new sub-query, use the existing sub-query
            # with the most dimensions as base
            assert max_query_id is not None
            return max_query_id

        return None

    def _patch_query_masks_with_base(
        self,
        query: CompiledQuery,
        split_masks: list[QuerySplitMask],
        expr_id_gen: PrefixedIdGen,
        query_id_gen: PrefixedIdGen,
    ) -> list[QuerySplitMask]:
        base_formula_split_masks = self._get_formula_mask_counterparts(
            query=query, split_masks=split_masks, expr_id_gen=expr_id_gen
        )

        # Find the indices of filters that will be applied at the upper level
        split_filter_indices = {
            fla_mask.formula_list_idx
            for query_mask in split_masks
            for fla_mask in query_mask.formula_split_masks
            if fla_mask.query_part == QueryPart.filters
        }
        base_filter_indices = set(range(len(query.filters))) - split_filter_indices

        base_subquery_candidate_id = self._find_base_query_candidate(
            query=query,
            split_masks=split_masks,
            base_formula_split_masks=base_formula_split_masks,
            base_filter_indices=base_filter_indices,
        )
        if base_subquery_candidate_id is not None:
            # Patch the candidate with `is_base=True` and return the original mask list
            split_masks = [
                (mask if mask.subquery_id is not base_subquery_candidate_id else mask.clone(is_base=True))
                for mask in split_masks
            ]
            # Re-order them so that the base is the first one (False goes before True)
            split_masks = sorted(split_masks, key=lambda mask: (not mask.is_base, split_masks.index(mask)))
            return split_masks

        # Generate additional formulas from query's dimensions
        new_add_formulas: list[AddFormulaInfo] = []
        base_dimension_extracts: set[NodeExtract] = set()
        for formula in query.group_by:
            alias = expr_id_gen.get_id()
            add_formula = AddFormulaInfo(
                alias=alias,
                expr=formula.formula_obj.expr,
                from_ids=frozenset(formula.avatar_ids),
                is_group_by=not inspect_expression.is_constant_expression(formula.formula_obj.expr),
            )
            new_add_formulas.append(add_formula)
            if add_formula.is_group_by:
                base_dimension_extracts.add(add_formula.expr.extract_not_none)

        new_filter_indices = {
            filter_idx for filter_idx in range(len(query.filters)) if filter_idx in base_filter_indices
        }

        base_mask = QuerySplitMask(
            subquery_type=SubqueryType.generated_base,
            subquery_id=query_id_gen.get_id(),
            formula_split_masks=tuple(base_formula_split_masks),
            add_formulas=tuple(new_add_formulas),
            filter_indices=frozenset(new_filter_indices),
            join_type=None,
            joining_node=None,
            is_base=True,
        )
        return [base_mask] + split_masks

    def _separate_base_and_other_masks(
        self,
        split_masks: list[QuerySplitMask],
    ) -> tuple[QuerySplitMask, list[QuerySplitMask]]:
        """Separate list of sub-query masks into base and all others."""

        assert split_masks

        base_mask = split_masks[0]
        assert base_mask.is_base

        other_masks = split_masks[1:]
        assert all(not other_mask.is_base for other_mask in other_masks)

        return base_mask, other_masks

    def optimize_query_split_masks(self, split_masks: list[QuerySplitMask]) -> list[QuerySplitMask]:
        """Dummy method that can be redefined in subclasses to optimize the creation of sub-queries"""
        return split_masks

    def _patch_mask_dimensions(
        self,
        split_masks: list[QuerySplitMask],
        expr_id_gen: PrefixedIdGen,
    ) -> list[QuerySplitMask]:
        """
        Find the base sub-query in the list and patch it with additional select items
        so that join expressions can be built correctly.
        """

        base_mask, other_masks = self._separate_base_and_other_masks(split_masks)

        base_add_formula_extracts: set[NodeExtract] = {
            add_formula.expr.extract_not_none for add_formula in base_mask.add_formulas
        }

        # Add extra dimensions (and other formulas needed for joins)
        # if and only if they are either constant expressions or not dimensions (are aggregations)
        # (for joining to sub-queries with total grouping - with aggregation, but without GROUP BY)
        for other_mask in other_masks:
            for add_formula in other_mask.add_formulas:
                if add_formula.expr.extract_not_none in base_add_formula_extracts:
                    continue  # Skip the ones that the mask already has

                new_add_formula = AddFormulaInfo(
                    alias=expr_id_gen.get_id(),
                    expr=add_formula.expr,
                    from_ids=add_formula.from_ids,
                    is_group_by=add_formula.is_group_by,
                )
                base_mask = base_mask.clone(add_formulas=base_mask.add_formulas + (new_add_formula,))
                base_add_formula_extracts.add(add_formula.expr.extract_not_none)

        return [base_mask] + other_masks

    def _get_query_patch_from_split_masks(
        self,
        query: CompiledQuery,
        split_masks: list[QuerySplitMask],
        query_id_gen: PrefixedIdGen,
        expr_id_gen: PrefixedIdGen,
    ) -> CompiledMultiQueryPatch:
        # Perform compatibility check before the list of query split masks is modified
        subqueries_compatible = self._are_subquery_dimensions_compatible(split_masks)

        # Create a mask for select items unaffected by the given masks.
        # It will represent the cropped original query.
        split_masks = self._patch_query_masks_with_base(
            query=query, split_masks=split_masks, expr_id_gen=expr_id_gen, query_id_gen=query_id_gen
        )
        split_masks = self.optimize_query_split_masks(split_masks=split_masks)
        split_masks = self._patch_mask_dimensions(split_masks=split_masks, expr_id_gen=expr_id_gen)

        # Generate subquery for each split-mask
        result_queries: list[CompiledQuery] = []
        for mask in split_masks:
            subquery = self._generate_subquery_for_mask(query=query, split_mask=mask)
            # Here some keys may appear for several subqueries.
            # Retain the value from the earliest appearance of the key.
            result_queries.append(subquery)

        # Crop original query (replace expressions specified by split masks with subquery fields)
        updated_original_query = self._crop_original_query(
            query=query, split_masks=split_masks, subqueries_compatible=subqueries_compatible
        )
        result_queries.append(updated_original_query)

        # Put it all into a patch object and return it
        patch = CompiledMultiQueryPatch(
            patch_multi_query=CompiledMultiQuery(queries=result_queries),
        )
        return patch

    def split_query(
        self,
        query: CompiledQuery,
        requirement_subtree: CompiledMultiQueryBase,
        query_id_gen: PrefixedIdGen,
        expr_id_gen: PrefixedIdGen,
    ) -> Optional[CompiledMultiQueryPatch]:
        split_masks = self.get_split_masks(query, expr_id_gen=expr_id_gen, query_id_gen=query_id_gen)
        if not split_masks:
            return None

        patch = self._get_query_patch_from_split_masks(
            query=query,
            split_masks=split_masks,
            query_id_gen=query_id_gen,
            expr_id_gen=expr_id_gen,
        )
        return patch
