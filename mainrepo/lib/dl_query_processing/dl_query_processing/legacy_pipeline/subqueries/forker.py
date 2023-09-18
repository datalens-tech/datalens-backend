from __future__ import annotations

import itertools
import logging
import re
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Collection,
    Dict,
    FrozenSet,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    cast,
)

import attr

from dl_constants.enums import JoinType
from dl_core.components.ids import (
    AvatarId,
    FieldId,
)
import dl_formula.core.aux_nodes as formula_aux_nodes
import dl_formula.core.exc as formula_exc
from dl_formula.core.extract import NodeExtract
import dl_formula.core.fork_nodes as formula_fork_nodes
from dl_formula.core.index import NodeHierarchyIndex
import dl_formula.core.nodes as formula_nodes
from dl_formula.inspect.expression import used_fields
from dl_query_processing.compilation.primitives import (
    CompiledJoinOnFormulaInfo,
    FromColumn,
    FromObject,
    JoinedFromObject,
    SubqueryFromObject,
)
from dl_query_processing.legacy_pipeline.separation.primitives import (
    CompiledMultiLevelQuery,
    MultiQueryIndex,
)
from dl_query_processing.legacy_pipeline.subqueries.grouping_normalizer import GroupByNormalizer
from dl_query_processing.legacy_pipeline.subqueries.query_tools import (
    CompiledMultiLevelQueryIncrementalPatch,
    CompiledMultiLevelQueryReplacementPatch,
    add_dummy_select_column,
    apply_multi_query_incremental_patches,
    apply_multi_query_replacement_patches,
    copy_and_remap_query,
    get_query_level_used_fields,
    remap_formula_obj_fields,
)
from dl_query_processing.legacy_pipeline.subqueries.sanitizer import MultiQuerySanitizer

if TYPE_CHECKING:
    from dl_query_processing.compilation.primitives import (
        CompiledFormulaInfo,
        CompiledOrderByFormulaInfo,
        CompiledQuery,
    )

LOGGER = logging.getLogger(__name__)

# For replacing `_f<num>` postfixes in forked sub-query IDs:
F_POSTFIXED_RE = re.compile(r"(?P<prefix>.*)_f\d+$")

_JOIN_TYPE_MAP = {
    formula_fork_nodes.JoinType.inner: JoinType.inner,
    formula_fork_nodes.JoinType.left: JoinType.left,
}


def str_not_none(s: Optional[str]) -> str:
    assert s is not None
    return s


class FullNodeIndex(NamedTuple):
    list_idx: int
    expr_idx: int
    node_idx: NodeHierarchyIndex


@attr.s
class SubqueryForkInfo:
    original_subquery_id: AvatarId = attr.ib(kw_only=True)
    joining_node: formula_fork_nodes.QueryForkJoiningBase = attr.ib(kw_only=True)
    bfb_field_ids: FrozenSet[FieldId] = attr.ib(kw_only=True)
    dimensions: Tuple[formula_nodes.FormulaItem, ...] = attr.ib(kw_only=True)
    join_type: JoinType = attr.ib(kw_only=True)
    required_select_aliases: Set[str] = attr.ib(kw_only=True, factory=set)
    full_node_indices: List[FullNodeIndex] = attr.ib(kw_only=True, factory=list)


class SubqueryForkSignature(NamedTuple):
    lod_idx: int  # A switch to disable grouping of same-dimension LODs together
    node_extract: NodeExtract
    bfb_names: FrozenSet[str]
    dim_extracts: FrozenSet[NodeExtract]
    join_type: JoinType


def _collect_alias_to_query_id_map(queries: Collection[CompiledQuery]) -> Dict[str, AvatarId]:
    alias_to_subquery_id_map: Dict[str, AvatarId] = {}  # Will be overridden
    for query in queries:
        for formula in query.select:
            assert formula.alias is not None
            alias_to_subquery_id_map[formula.alias] = query.id
    return alias_to_subquery_id_map


@attr.s
class QueryForker:
    _verbose_logging: bool = attr.ib(kw_only=True, default=False)
    _subquery_counter: Iterator[int] = attr.ib(init=False, factory=itertools.count)

    def _log_info(self, *args: Any, **kwargs: Any) -> None:
        if self._verbose_logging:
            LOGGER.info(*args, **kwargs)

    def _validate_dimensions_and_extract_field_names(
        self, dimensions: Tuple[formula_nodes.FormulaItem, ...]
    ) -> Set[str]:
        dimension_aliases: Set[str] = set()
        for dim in dimensions:
            if isinstance(dim, formula_nodes.Field):
                # Main case
                dimension_aliases.add(dim.name)
            elif isinstance(dim, (formula_nodes.BaseLiteral, formula_aux_nodes.ErrorNode)):
                pass  # Corner cases
                # Literals as dimensions can be safely ignored.
                # Error nodes will still raise errors in the main subquery.
            else:  # Invalid case
                raise TypeError(f"Expected to see Field node as dimension, " f"but got {type(dim).__name__}")
        return dimension_aliases

    def _collect_ancestors_by_level(
        self,
        compiled_multi_query: CompiledMultiLevelQuery,
        subquery_idx: MultiQueryIndex,
    ) -> Dict[int, List[CompiledQuery]]:
        parents_by_level: Dict[int, List[CompiledQuery]] = {}
        upper_level_child_queries = [compiled_multi_query[subquery_idx]]
        # Collect info recursively (from the top down)
        for sub_level_idx in reversed(range(subquery_idx.level_idx)):
            all_parent_ids = set(
                from_id for child in upper_level_child_queries for from_id in child.joined_from.iter_ids()
            )

            # Generate list of queries to be used as children for the next level
            parents = [
                query for query in compiled_multi_query.levels[sub_level_idx].queries if query.id in all_parent_ids
            ]
            parents_by_level[sub_level_idx] = parents
            upper_level_child_queries = parents

        return parents_by_level

    def _fork_all_parents(
        self,
        *,
        compiled_multi_query: CompiledMultiLevelQuery,
        subquery_idx: MultiQueryIndex,
    ) -> Tuple[CompiledMultiLevelQueryIncrementalPatch, Dict[str, str], Dict[AvatarId, AvatarId]]:
        level_idx = subquery_idx.level_idx

        patch = CompiledMultiLevelQueryIncrementalPatch.generate(level_cnt=level_idx + 1)

        # Build query dependencies
        parents_by_level = self._collect_ancestors_by_level(
            compiled_multi_query=compiled_multi_query, subquery_idx=subquery_idx
        )

        # Clone them from the bottom up
        remapped_avatar_ids: Dict[AvatarId, AvatarId] = {}
        old_all_remapped_parent_aliases: Dict[str, str] = {}
        for sub_level_idx in range(level_idx):
            new_all_remapped_parent_aliases: Dict[str, str] = {}
            for parent in parents_by_level[sub_level_idx]:
                new_parent_id = self._make_id_for_forked_subquery(original_subquery=parent)

                # Generate set of used query IDs to replace used_avatar_ids in the query clone
                cloned_parent, remapped_parent_aliases = copy_and_remap_query(
                    parent,
                    id=new_parent_id,
                    field_name_map=old_all_remapped_parent_aliases,
                    avatar_map=remapped_avatar_ids,
                )
                remapped_avatar_ids[parent.id] = new_parent_id
                new_all_remapped_parent_aliases.update(remapped_parent_aliases)
                patch.add_query_for_level(level_idx=sub_level_idx, compiled_query=cloned_parent)

            # Rotate the mapping
            old_all_remapped_parent_aliases = new_all_remapped_parent_aliases

        return patch, old_all_remapped_parent_aliases, remapped_avatar_ids

    def _fork_subquery(
        self,
        *,
        compiled_multi_query: CompiledMultiLevelQuery,
        subquery_idx: MultiQueryIndex,
        required_select_aliases: Set[str],
        bfb_field_ids: FrozenSet[FieldId],
        dimensions: Tuple[formula_nodes.FormulaItem, ...],
    ) -> Tuple[CompiledMultiLevelQueryIncrementalPatch, Dict[str, str]]:
        """
        Fork sub-query and, possibly, parent sub-queries.
        Return a multi-query patch and a dict of remapped aliases for this sub-query.
        """

        level_idx = subquery_idx.level_idx
        original_subquery = compiled_multi_query[subquery_idx]
        forked_query_id = self._make_id_for_forked_subquery(original_subquery=original_subquery)

        # Extract dimensions from subquery
        dimension_aliases = self._validate_dimensions_and_extract_field_names(dimensions)

        # All dimensions must always be selected to be used in JOINs
        self._log_info(f"Using required select aliases {required_select_aliases} in forked subquery {forked_query_id}")
        self._log_info(f"Adding dimensions {dimension_aliases} to required select aliases from group_by")
        required_select_aliases = required_select_aliases | dimension_aliases

        # Generate SELECTs based on required_select_aliases.
        # Because all ORDER BY expressions must be present in SELECT,
        # It's easier to assume that there is no ORDER BY since this is not the top level
        assert not original_subquery.order_by
        stripped_query_select: List[CompiledFormulaInfo] = [
            expr for expr in original_subquery.select if expr.alias in required_select_aliases
        ]
        assert not original_subquery.order_by  # See comment above.
        stripped_query_order_by: List[CompiledOrderByFormulaInfo] = []

        # Generate GROUP BYs.
        stripped_query_group_by: List[CompiledFormulaInfo] = []
        # Use only expressions from group_by that are present in specified dimensions.
        used_group_by_aliases = set()
        for expr in original_subquery.group_by:
            if expr.alias in dimension_aliases:
                stripped_query_group_by.append(expr)
                used_group_by_aliases.add(expr.alias)
        # Add additional dimensions that are not preset in the original query's GROUP BY.
        # They must be present in SELECT
        for expr in original_subquery.select:
            if expr.alias in dimension_aliases and expr.alias not in used_group_by_aliases:
                stripped_query_group_by.append(expr)
                used_group_by_aliases.add(expr.alias)

        # Filter filters.
        # (Remove the ones that are specified in BFB clauses)
        # Note that filters don't use aliases, so no remapping is needed.
        # First collect all IDs from BFB clauses
        stripped_query_filters = [
            expr for expr in original_subquery.filters if expr.original_field_id not in bfb_field_ids
        ]

        stripped_original_query = original_subquery.clone(
            select=stripped_query_select,
            group_by=stripped_query_group_by,
            filters=stripped_query_filters,
            order_by=stripped_query_order_by,
        )

        patch, remapped_parent_aliases, remapped_avatars = self._fork_all_parents(
            compiled_multi_query=compiled_multi_query,
            subquery_idx=subquery_idx,
        )

        forked_query, remapped_aliases = copy_and_remap_query(
            stripped_original_query,
            id=forked_query_id,
            field_name_map=remapped_parent_aliases,
            avatar_map=remapped_avatars,
        )

        patch.add_query_for_level(level_idx, forked_query)

        self._log_info(f"Making forked sub-query {forked_query_id} with alias mapping: {remapped_aliases}")

        return patch, remapped_aliases

    def _make_id_for_forked_subquery(self, *, original_subquery: CompiledQuery) -> str:
        # Generate new query's ID
        # "f" stands for "fork"
        prefix = original_subquery.id
        f_postfix_match = F_POSTFIXED_RE.match(prefix)
        if f_postfix_match is not None:
            prefix = f_postfix_match.group("prefix")
        forked_query_id: AvatarId = f"{prefix}_f{next(self._subquery_counter)}"
        return forked_query_id

    def _make_alias_for_dummy_coulumn(self, *, original_subquery: CompiledQuery) -> str:
        return f"{original_subquery.id}_d_0"

    def _make_fork_join_on_expression(
        self,
        original_subquery: CompiledQuery,
        fork_subquery: CompiledQuery,
        joining_node: formula_fork_nodes.QueryForkJoiningBase,
        remapped_forked_aliases: Dict[str, str],
        join_type: JoinType = JoinType.left,
    ) -> Optional[CompiledJoinOnFormulaInfo]:
        def and_part(condition: Optional[formula_nodes.Binary], part: formula_nodes.Binary) -> formula_nodes.Binary:
            if condition is None:
                return part
            return formula_nodes.Binary.make(name="and", left=condition, right=part)

        join_expr: Optional[formula_nodes.Binary] = None

        if isinstance(joining_node, formula_fork_nodes.QueryForkJoiningWithList):
            # Joining node explicitly lists joining conditions.
            for condition in joining_node.condition_list:
                fork_expr: formula_nodes.FormulaItem
                original_expr: formula_nodes.FormulaItem
                if isinstance(condition, formula_fork_nodes.SelfEqualityJoinCondition):
                    expr = condition.expr
                    if not isinstance(expr, (formula_nodes.Field, formula_aux_nodes.ErrorNode)):
                        raise TypeError(f"Expected field or error node, got {type(expr)}")

                    original_expr = expr
                    fork_expr = expr
                elif isinstance(condition, formula_fork_nodes.BinaryJoinCondition):
                    original_expr = condition.expr
                    fork_expr = condition.fork_expr
                else:
                    raise TypeError(f"Type {type(condition).__name__} is not supported")

                # Remap columns in for_expr (right side of the condition)
                # because it has to be evaluated against the forked query, and not the original one
                fork_expr = remap_formula_obj_fields(node=fork_expr, field_name_map=remapped_forked_aliases)
                part = formula_nodes.Binary.make(name="_==", left=original_expr, right=fork_expr)
                join_expr = and_part(condition=join_expr, part=part)

        else:
            raise TypeError(f"Joining node type {type(joining_node).__name__} is not supported")

        if join_expr is None:
            return None

        return CompiledJoinOnFormulaInfo(
            alias=None,  # Will not be used
            formula_obj=formula_nodes.Formula.make(expr=join_expr),
            avatar_ids={original_subquery.id, fork_subquery.id},
            original_field_id=None,
            left_id=original_subquery.id,
            right_id=fork_subquery.id,
            join_type=join_type,
        )

    def _are_subquery_dimensions_compatible(
        self, subquery_infos: Dict[SubqueryForkSignature, SubqueryForkInfo]
    ) -> bool:
        if not subquery_infos:
            return True

        dimension_sets = [subquery_info_key.dim_extracts for subquery_info_key in subquery_infos]
        max_dim_set = frozenset(itertools.chain.from_iterable(dimension_sets))
        return max_dim_set in dimension_sets

    def _get_child_dimension_sets(
        self,
        node: formula_nodes.FormulaItem,
        alias_to_subquery_id_map: Dict[str, AvatarId],
        compiled_multi_query: CompiledMultiLevelQuery,
    ) -> FrozenSet[FrozenSet[NodeExtract]]:
        """
        Get dimension sets of child QueryFork's (retrieved subqueries)
        for the given formula node (most likely a QueryFork itself).
        """
        used_field_nodes = used_fields(node)
        child_dimension_sets: Set[FrozenSet[NodeExtract]] = set()
        for field_node in used_field_nodes:
            subq_id = alias_to_subquery_id_map[field_node.name]
            subq_idx = compiled_multi_query.get_query_index_by_id(id=subq_id)
            subq = compiled_multi_query[subq_idx]
            field_child_expr = next(expr for expr in subq.select if expr.alias == field_node.name)
            for _child_qfork_idx, child_qfork in field_child_expr.formula_obj.enumerate():
                if isinstance(child_qfork, formula_fork_nodes.QueryFork):
                    lod = child_qfork.lod
                    if isinstance(lod, formula_nodes.FixedLodSpecifier):
                        dim_set = frozenset(dim.extract for dim in lod.dim_list)
                        assert all(dim_extr is not None for dim_extr in dim_set)
                        child_dimension_sets.add(cast(FrozenSet[NodeExtract], dim_set))

        return frozenset(child_dimension_sets)

    def _collect_query_fork_infos(
        self,
        expr_lists: Tuple[List[CompiledFormulaInfo], ...],
        alias_to_subquery_id_map: Dict[str, AvatarId],
        compiled_multi_query: CompiledMultiLevelQuery,
        # Because grouping LODs together sometimes breaks them:
        disable_grouping_lods: bool = False,
    ) -> Dict[SubqueryForkSignature, SubqueryForkInfo]:
        subquery_infos_by_joining: Dict[SubqueryForkSignature, SubqueryForkInfo] = {}
        lod_idx_counter = itertools.count()
        lod_idx_by_child_dims: Dict[FrozenSet[FrozenSet[NodeExtract]], int] = {}

        def gen_new_lod_idx() -> int:
            return next(lod_idx_counter)

        def _get_lod_idx_for_query_fork(qfork_node: formula_fork_nodes.QueryFork) -> int:
            if disable_grouping_lods:
                # LODs shouldn't be grouped, so generate a new index every time
                return gen_new_lod_idx()

            # Check wheher dimensions of child queries are compatible.
            # If they are, then LOD grouping is possible
            child_dimension_sets = self._get_child_dimension_sets(
                node=qfork_node,
                alias_to_subquery_id_map=alias_to_subquery_id_map,
                compiled_multi_query=compiled_multi_query,
            )
            if child_dimension_sets not in lod_idx_by_child_dims:
                lod_idx_by_child_dims[child_dimension_sets] = gen_new_lod_idx()
            return lod_idx_by_child_dims[child_dimension_sets]

        # Collect info about all QueryForks in the query
        for expr_list_idx, expr_list in enumerate(expr_lists):
            for expr_idx, expr in enumerate(expr_list):
                for node_idx, node in expr.formula_obj.enumerate():
                    if isinstance(node, formula_fork_nodes.QueryFork):
                        full_node_idx = FullNodeIndex(list_idx=expr_list_idx, expr_idx=expr_idx, node_idx=node_idx)
                        joining_node = node.joining
                        # Compile a set of expression aliases that are required to be selected
                        # in the forked subquery (the clone of the original).
                        # All other select items will be dropped
                        significant_expressions = [node.result_expr]
                        if isinstance(joining_node, formula_fork_nodes.QueryForkJoiningWithList):
                            for joining_condition in joining_node.condition_list:
                                if isinstance(joining_condition, formula_fork_nodes.BinaryJoinCondition):
                                    significant_expressions.append(joining_condition.fork_expr)
                                elif isinstance(joining_condition, formula_fork_nodes.SelfEqualityJoinCondition):
                                    significant_expressions.append(joining_condition.expr)

                        # Find the original sub-query ID
                        required_select_aliases = {
                            used_fld.name for expr in significant_expressions for used_fld in used_fields(expr)
                        }
                        required_subquery_ids = {alias_to_subquery_id_map[alias] for alias in required_select_aliases}
                        assert len(required_subquery_ids) == 1, "Cannot fork more than 1 subquery at a time"
                        original_subquery_id = next(iter(required_subquery_ids))

                        join_type = _JOIN_TYPE_MAP[node.join_type]
                        lod = node.lod
                        dim_list: Tuple[formula_nodes.FormulaItem, ...]
                        if isinstance(lod, formula_nodes.FixedLodSpecifier):
                            dim_list = tuple(lod.dim_list)
                        elif isinstance(lod, formula_nodes.InheritedLodSpecifier):
                            # Inherit (copy) original query's dimension list
                            original_subquery_idx = compiled_multi_query.get_query_index_by_id(original_subquery_id)
                            original_subquery = compiled_multi_query[original_subquery_idx]
                            dim_list = tuple(
                                formula_nodes.Field.make(str_not_none(fla.alias)) for fla in original_subquery.group_by
                            )
                        else:
                            raise TypeError(f"Unsupported LodSpecifier type: {type(lod).__name__}")

                        # Create a hashable key that will be unique for each query fork.
                        subquery_info_key = SubqueryForkSignature(
                            # Use different `lod_idx` value every time to disable LOD grouping
                            lod_idx=_get_lod_idx_for_query_fork(qfork_node=node),
                            node_extract=joining_node.extract,  # type: ignore
                            bfb_names=node.before_filter_by.field_names,
                            dim_extracts=frozenset(dim.extract for dim in dim_list),  # type: ignore
                            join_type=join_type,
                        )

                        if subquery_info_key not in subquery_infos_by_joining:
                            # This is the first time a fork with this signature was encountered
                            subquery_infos_by_joining[subquery_info_key] = SubqueryForkInfo(
                                original_subquery_id=original_subquery_id,
                                joining_node=joining_node,
                                bfb_field_ids=node.before_filter_by.field_names,
                                dimensions=dim_list,
                                join_type=join_type,
                            )

                        subquery_info = subquery_infos_by_joining[subquery_info_key]
                        assert subquery_info.original_subquery_id == original_subquery_id, (
                            f"Subquery ID mismatch: expected {subquery_info.original_subquery_id}, "
                            f"got {original_subquery_id}"
                        )
                        subquery_info.required_select_aliases |= required_select_aliases
                        subquery_info.full_node_indices.append(full_node_idx)

        return subquery_infos_by_joining

    def _scan_subquery_and_make_forks(
        self,
        *,
        compiled_multi_query: CompiledMultiLevelQuery,
        multi_query_idx: MultiQueryIndex,
        alias_to_subquery_id_map: Dict[str, AvatarId],
        # Switch for turning the fork optimization on or off.
        # Off at the moment because doesn't work correctly in some cases
        allow_skip_fork: bool = False,
    ) -> Tuple[
        CompiledQuery, List[CompiledMultiLevelQueryIncrementalPatch], List[CompiledMultiLevelQueryReplacementPatch]
    ]:
        # Incremental patches (new sub-queries being added to the multi-query)
        incremental_patches: List[CompiledMultiLevelQueryIncrementalPatch] = []
        # Replacement patches (some existing sub-queries need to have their `select`s modified)
        replacement_patches: List[CompiledMultiLevelQueryReplacementPatch] = []

        # The sub-query that we will be scanning for `QueryFork`s.
        # As a result, new sub-queries will be added to the previous level
        # (referenced by this sub-query) via patches returned from this function.
        compiled_flat_query = compiled_multi_query[multi_query_idx]

        expr_lists: Tuple[List[CompiledFormulaInfo], ...] = (  # type: ignore
            compiled_flat_query.select[:],
            compiled_flat_query.group_by[:],
            compiled_flat_query.order_by[:],
            compiled_flat_query.filters[:],
            compiled_flat_query.join_on[:],
        )

        subquery_infos_by_joining = self._collect_query_fork_infos(
            expr_lists=expr_lists,
            alias_to_subquery_id_map=alias_to_subquery_id_map,
            compiled_multi_query=compiled_multi_query,
        )
        self._log_info(f"Detected {len(subquery_infos_by_joining)} subquery specs in query {compiled_flat_query.id}")
        dims_compatible = self._are_subquery_dimensions_compatible(subquery_infos=subquery_infos_by_joining)
        if not dims_compatible:
            self._log_info("Sub-query LODs are incompatible")

        def _find_subquery(subquery_info: SubqueryForkInfo) -> Tuple[MultiQueryIndex, CompiledQuery]:
            subquery_idx = compiled_multi_query.get_query_index_by_id(id=subquery_info.original_subquery_id)
            return subquery_idx, compiled_multi_query[subquery_idx]

        skip_forking = False
        # Optimizations are available only if there is one subquery
        if len(subquery_infos_by_joining) == 1:
            subquery_info = next(iter(subquery_infos_by_joining.values()))
            _, original_subquery = _find_subquery(subquery_info)
            fork_dimensions = self._validate_dimensions_and_extract_field_names(subquery_info.dimensions)
            original_subquery_dimensions = {dim_obj.alias for dim_obj in original_subquery.group_by}
            if subquery_info.joining_node.is_self_eq_join and fork_dimensions == original_subquery_dimensions:
                # Fork has the same dimensions as the original subquery,
                # so there is no need to create the forked subquery.
                skip_forking = allow_skip_fork

        all_new_used_avatar_ids: Set[AvatarId] = set(compiled_flat_query.joined_from.iter_ids())
        new_join_on_expressions: List[CompiledJoinOnFormulaInfo] = []

        for _subquery_info_key, subquery_info in subquery_infos_by_joining.items():
            joining_node = subquery_info.joining_node
            new_expr_avatar_ids: Set[AvatarId] = set()
            all_remapped_forked_aliases: Dict[str, str] = {}

            if not skip_forking:
                # Make sure that all dimensions have turned into simple fields in the sliced query
                # (with the exception of a couple of corner cases)
                dimension_aliases = self._validate_dimensions_and_extract_field_names(subquery_info.dimensions)

                # First collect all sub-queries referenced by found QueryFork nodes
                subquery_idx, original_subquery = _find_subquery(subquery_info)
                required_select_aliases = subquery_info.required_select_aliases

                # If the joining clause is empty (zero-dimension LOD),
                # then we need to add a dummy column to use in the JOIN ON clause.
                # Use an integer constant.
                # This will generate a replacement patch: original subquery must be replaced
                # with a copy with the dummy column added to the `select` list.
                if (
                    isinstance(joining_node, formula_fork_nodes.QueryForkJoiningWithList)
                    and not joining_node.condition_list
                ):
                    # A zero-dimension LOD
                    dummy_col_alias = self._make_alias_for_dummy_coulumn(original_subquery=original_subquery)
                    select_aliases = {formula.alias for formula in original_subquery.select}
                    if dummy_col_alias not in select_aliases:
                        # Add dummy column to original sub-query only if it is not already there
                        original_subquery = add_dummy_select_column(original_subquery, alias=dummy_col_alias)
                        new_repl_patch = CompiledMultiLevelQueryReplacementPatch(
                            subquery_idx=subquery_idx,
                            new_subquery=original_subquery,
                        )
                        # Apply the replacement patch locally for all the forking to work correctly,
                        # but also add it to the list that will be returned from this function,
                        # so that it applied to the overall result.
                        compiled_multi_query = apply_multi_query_replacement_patches(
                            compiled_multi_query,
                            patches=[new_repl_patch],
                        )
                        replacement_patches.append(new_repl_patch)

                    joining_node = formula_fork_nodes.QueryForkJoiningWithList.make(
                        condition_list=[
                            formula_fork_nodes.SelfEqualityJoinCondition.make(
                                expr=formula_nodes.Field.make(name=dummy_col_alias)
                            )
                        ]
                    )
                    # Mark as required for selection - otherwise it will be
                    # ignored and removed in the forked sub-query.
                    required_select_aliases.add(dummy_col_alias)

                if subquery_idx != 0:  # Is not the main subquery of the level
                    updated_original_group_by = original_subquery.group_by
                    original_gb_aliases = {formula.alias for formula in updated_original_group_by}
                    additional_gb_aliases = sorted(dimension_aliases - original_gb_aliases)
                    if additional_gb_aliases:
                        select_formulas_by_alias = {formula.alias: formula for formula in original_subquery.select}
                        updated_original_group_by = updated_original_group_by + [
                            select_formulas_by_alias[alias] for alias in additional_gb_aliases
                        ]
                    if updated_original_group_by is not original_subquery.group_by:
                        original_subquery = original_subquery.clone(group_by=updated_original_group_by)
                        new_repl_patch = CompiledMultiLevelQueryReplacementPatch(
                            subquery_idx=subquery_idx,
                            new_subquery=original_subquery,
                        )
                        compiled_multi_query = apply_multi_query_replacement_patches(
                            compiled_multi_query,
                            patches=[new_repl_patch],
                        )
                        replacement_patches.append(new_repl_patch)

                # Fork referenced sub-query (duplicate query object).
                # Also collect JOIN expressions that will be needed to put all this together
                self._log_info(f"Going to fork subquery {original_subquery.id}")
                multi_query_patch, remapped_forked_aliases = self._fork_subquery(
                    compiled_multi_query=compiled_multi_query,
                    subquery_idx=subquery_idx,
                    required_select_aliases=subquery_info.required_select_aliases,
                    bfb_field_ids=subquery_info.bfb_field_ids,
                    dimensions=subquery_info.dimensions,
                )
                incremental_patches.append(multi_query_patch)
                forked_subquery = multi_query_patch.top_level_query
                fork_join_on_expression = self._make_fork_join_on_expression(
                    original_subquery=original_subquery,
                    fork_subquery=forked_subquery,
                    joining_node=joining_node,
                    remapped_forked_aliases=remapped_forked_aliases,
                    join_type=subquery_info.join_type,
                )
                if fork_join_on_expression is not None:
                    new_join_on_expressions.append(fork_join_on_expression)
                self._log_info(f"Added new forked subquery: {forked_subquery.id}")
                all_remapped_forked_aliases.update(remapped_forked_aliases)
                all_new_used_avatar_ids.add(forked_subquery.id)
                new_expr_avatar_ids.add(forked_subquery.id)

            # Replace all QueryFork nodes with their result_expr nodes
            # referencing the forked sub-queries
            for full_node_idx in subquery_info.full_node_indices:
                expr_list_idx, expr_idx, node_idx = full_node_idx
                old_expr: CompiledFormulaInfo = expr_lists[expr_list_idx][expr_idx]
                old_formula = old_expr.formula_obj
                # Apply column remapping to the result_expr
                old_node = old_formula[node_idx]
                assert isinstance(old_node, formula_fork_nodes.QueryFork), f"Expected QueryFork, got {old_node}"

                new_result_expr_node: formula_nodes.FormulaItem
                if not dims_compatible:
                    new_result_expr_node = formula_aux_nodes.ErrorNode.make(
                        message="LOD dimensions are incompatible",
                        err_code=formula_exc.LodIncompatibleDimensionsError.default_code,
                    )
                elif skip_forking:
                    new_result_expr_node = old_node.result_expr
                else:
                    new_result_expr_node = remap_formula_obj_fields(
                        node=old_node.result_expr,
                        field_name_map=all_remapped_forked_aliases,
                    )

                # Replace the original ForkQuery node with the patched result_expr
                new_expr = old_expr.clone(
                    formula_obj=old_formula.replace_at_index(index=node_idx, expr=new_result_expr_node),
                    avatar_ids=old_expr.avatar_ids | new_expr_avatar_ids,
                )
                expr_lists[expr_list_idx][expr_idx] = new_expr

        select, group_by, order_by, filters, join_on = expr_lists
        join_on = join_on + new_join_on_expressions  # type: ignore

        # Build the joined FROM
        froms: list[FromObject] = []
        level_idx = multi_query_idx.level_idx
        assert level_idx != 0
        # Build a dict of sub-queries
        subqueries = {query.id: query for query in compiled_multi_query.levels[level_idx - 1].queries}
        for patch in incremental_patches:
            for level_patch in patch.level_patches:
                for query in level_patch:
                    subqueries[query.id] = query
        # And use it to generate column lists for sub-query `FromObject`s
        for from_id in sorted(all_new_used_avatar_ids):
            subquery = subqueries[from_id]
            froms.append(
                SubqueryFromObject(
                    id=from_id,
                    query_id=from_id,
                    alias=from_id,
                    columns=tuple(
                        FromColumn(id=select_formula.not_none_alias, name=select_formula.not_none_alias)
                        for select_formula in subquery.select
                    ),
                )
            )

        joined_from = JoinedFromObject(root_from_id=compiled_flat_query.joined_from.root_from_id, froms=froms)

        updated_compiled_flat_query = compiled_flat_query.clone(
            select=select,
            group_by=group_by,
            order_by=order_by,
            filters=filters,
            join_on=join_on,
            joined_from=joined_from,
        )
        return updated_compiled_flat_query, incremental_patches, replacement_patches

    def scan_and_fork_multi_query(
        self,
        *,
        compiled_multi_query: CompiledMultiLevelQuery,
    ) -> CompiledMultiLevelQuery:
        sanitizer = MultiQuerySanitizer()

        # Iterate by index instead of directly
        # because compiled_multi_query will be updated on-the-go

        updated_queries: List[CompiledQuery]
        upper_level_used_aliases: Optional[AbstractSet[str]] = None  # Aliases used by the next query level
        sanitize_select = False

        for level_idx in reversed(range(len(compiled_multi_query.levels))):
            compiled_level = compiled_multi_query.levels[level_idx]

            if level_idx == 0:
                # Lowest level. Subquery forks are not applicable here.
                # Query forks start from the second level (ind=1)
                pass
            else:
                # Make mapping of expression aliases from lower-level queries (<alias: query_id>)
                prev_level_queries = compiled_multi_query.levels[level_idx - 1].queries
                alias_to_subquery_id_map = _collect_alias_to_query_id_map(prev_level_queries)

                updated_queries = []
                for query_idx, _compiled_flat_query in enumerate(compiled_level.queries):
                    mq_idx = MultiQueryIndex(level_idx=level_idx, query_idx=query_idx)
                    updated_query, incremental_patches, replacement_patches = self._scan_subquery_and_make_forks(
                        compiled_multi_query=compiled_multi_query,
                        multi_query_idx=mq_idx,
                        alias_to_subquery_id_map=alias_to_subquery_id_map,
                    )
                    updated_queries.append(updated_query)

                    # Apply lower-level patches to multi-query
                    compiled_multi_query = apply_multi_query_incremental_patches(
                        compiled_multi_query=compiled_multi_query,
                        patches=incremental_patches,
                    )
                    compiled_multi_query = apply_multi_query_replacement_patches(
                        compiled_multi_query=compiled_multi_query,
                        patches=replacement_patches,
                    )

                # Update level object
                compiled_level = compiled_level.clone(queries=updated_queries)

            # Clean up unnecessary select items, and unused sub-queries
            compiled_level = sanitizer.sanitize_compiled_level(
                compiled_level=compiled_level,
                used_aliases=upper_level_used_aliases,
                sanitize_select=sanitize_select,
                is_bottom_level=level_idx == 0,
            )

            if compiled_multi_query.levels[level_idx] is not compiled_level:
                # Either multi-query or level has been updated
                # Apply updated level to multi-query
                levels = compiled_multi_query.levels[:]
                levels[level_idx] = compiled_level
                compiled_multi_query = compiled_multi_query.clone(levels=levels)

            upper_level_used_aliases = get_query_level_used_fields(compiled_level)
            sanitize_select = True

        if compiled_multi_query.level_count() > 1:  # Only applicable to multi-level queries
            # FIXME: devise a "prevention" approach instead of "fixing"
            grouping_normalizer = GroupByNormalizer()
            compiled_multi_query = grouping_normalizer.normalize_multi_query_grouping(
                compiled_multi_query=compiled_multi_query
            )

        return compiled_multi_query
