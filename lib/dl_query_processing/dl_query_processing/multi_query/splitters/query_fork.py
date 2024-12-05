from collections import OrderedDict
import itertools
from typing import Any

import attr

from dl_constants.enums import JoinType
from dl_formula.core.extract import NodeExtract
import dl_formula.core.fork_nodes as formula_fork_nodes
from dl_formula.core.index import NodeHierarchyIndex
import dl_formula.core.nodes as formula_nodes
from dl_formula.inspect.env import InspectionEnvironment
import dl_formula.inspect.expression as inspect_expression
from dl_formula.inspect.expression import contains_node
import dl_formula.inspect.node as inspect_node
from dl_formula.mutation.mutation import (
    FormulaMutation,
    apply_mutations,
)
from dl_query_processing.compilation.primitives import CompiledQuery
from dl_query_processing.enums import QueryPart
from dl_query_processing.multi_query.splitters.mask_based import (
    AddFormulaInfo,
    AliasedFormulaSplitMask,
    FormulaSplitMask,
    MultiQuerySplitter,
    QuerySplitMask,
    SubqueryType,
)
from dl_query_processing.utils.name_gen import PrefixedIdGen


_JOIN_TYPE_MAP = {
    formula_fork_nodes.JoinType.inner: JoinType.inner,
    formula_fork_nodes.JoinType.left: JoinType.left,
}


@attr.s(frozen=True, auto_attribs=True)
class SubqueryForkSignature:
    lod_idx: int  # A switch to disable grouping of same-dimension LODs together
    joining_node_extract: NodeExtract
    bfb_names: frozenset[str]
    dim_extracts: frozenset[NodeExtract]
    child_lod_extracts: frozenset[NodeExtract]
    join_type: JoinType


@attr.s(frozen=True)
class ReplacementFormulaMutation(FormulaMutation):
    original: formula_nodes.FormulaItem = attr.ib()
    replacement: formula_nodes.FormulaItem = attr.ib()

    def match_node(self, node: formula_nodes.FormulaItem, parent_stack: tuple[formula_nodes.FormulaItem, ...]) -> bool:
        return node == self.original

    def make_replacement(
        self, old: formula_nodes.FormulaItem, parent_stack: tuple[formula_nodes.FormulaItem, ...]
    ) -> formula_nodes.FormulaItem:
        return self.replacement


@attr.s(frozen=True)
class QueryForkInfo:
    subquery_type: SubqueryType = attr.ib(kw_only=True)
    joining_node: formula_fork_nodes.QueryForkJoiningBase = attr.ib(kw_only=True)
    bfb_field_ids: frozenset[str] = attr.ib(kw_only=True)
    add_formulas: tuple[AddFormulaInfo, ...] = attr.ib(kw_only=True)
    bfb_filter_mutations: tuple[ReplacementFormulaMutation, ...] = attr.ib(kw_only=True)
    join_type: JoinType = attr.ib(kw_only=True)
    aliases_by_extract: dict[NodeExtract, str] = attr.ib(kw_only=True, factory=dict)
    formula_split_masks: list[AliasedFormulaSplitMask] = attr.ib(kw_only=True, factory=list)


# Some type aliases
FMask_QFork = tuple[FormulaSplitMask, formula_fork_nodes.QueryFork]
FMask_QFork_BFB = tuple[FormulaSplitMask, formula_fork_nodes.QueryFork, frozenset[str]]


@attr.s
class QueryForkQuerySplitter(MultiQuerySplitter):
    def _get_child_lod_extracts(
        self,
        node: formula_nodes.FormulaItem,
    ) -> frozenset[NodeExtract]:
        """
        Get LODs of child QueryFork's (retrieved subqueries)
        for the given formula node (most likely a QueryFork itself).
        This is done to check compatibility of query forks - whether or not
        they can be put in the same sub-query.
        """
        child_lod_extracts: set[NodeExtract] = set()
        child_query_forks = self._get_query_forks_from_expression(
            node=node,
            index_prefix=NodeHierarchyIndex(),
            query_part=QueryPart.select,
            formula_idx=0,  # Fake values, but they don't matter
        )
        for _, child_qfork in child_query_forks:
            child_lod_extracts.add(child_qfork.lod.extract_not_none)

        return frozenset(child_lod_extracts)

    def _get_query_forks_from_expression(
        self,
        node: formula_nodes.FormulaItem,
        index_prefix: NodeHierarchyIndex,
        query_part: QueryPart,
        formula_idx: int,
    ) -> list[tuple[FormulaSplitMask, formula_fork_nodes.QueryFork]]:
        result: list[tuple[FormulaSplitMask, formula_fork_nodes.QueryFork]] = []
        if isinstance(node, formula_fork_nodes.QueryFork):
            result.append(
                (
                    FormulaSplitMask(
                        query_part=query_part,
                        formula_list_idx=formula_idx,
                        outer_node_idx=index_prefix,
                        inner_node_idx=index_prefix + (1,),  # QueryFork.result_expr = Child(1)
                    ),
                    node,
                )
            )
        else:
            for child_index, child, _stack in inspect_expression.enumerate_autonomous_children(
                node, prefix=index_prefix
            ):
                result += self._get_query_forks_from_expression(
                    node=child,
                    index_prefix=child_index,
                    query_part=query_part,
                    formula_idx=formula_idx,
                )

        return result

    def _prioritize_and_filter_query_forks(
        self,
        fmask_qfork_bfb_list: list[FMask_QFork_BFB],
    ) -> tuple[SubqueryType, list[FMask_QFork_BFB]]:
        """
        Filter the found query forks.

        It is possible to minimize the number of JOINs in a query with window functions
        if we prioritize them and do not mix aggregation and lookup QueryForks with window function ones.
        If at the top level of a query we find WF and non-WF query forks,
        then we ignore the non-WF ones at this iteration.
        This way we get one sub-query above the split line and one below. Without any joins.

        All of this also requires the BFB of the top-level WF to be a sub-set of all other BFBs,
        otherwise this nesting is not possible
        """

        # Attempt to prioritize query forks generated by window functions.
        wf_qforks_found: bool = False
        wf_bfb_set: set[frozenset[str]] = set()
        non_wf_bfb_set: set[frozenset[str]] = set()
        for _formula_split_mask, qfork_node, normalized_bfb in fmask_qfork_bfb_list:
            if inspect_node.qfork_is_window(qfork_node):
                wf_qforks_found = True
                wf_bfb_set.add(normalized_bfb)
            else:
                non_wf_bfb_set.add(normalized_bfb)

        winfunc_mode: bool = False
        smallest_wf_bfb: frozenset[str] = frozenset()
        if wf_qforks_found:
            winfunc_mode = True
            # Check whether the smallest WF BFB is a subset of all other (WF and non-WF) BFBs
            smallest_wf_bfb = sorted(wf_bfb_set, key=lambda bfb: len(bfb))[0]
            for other_bfb in wf_bfb_set | non_wf_bfb_set:
                if not smallest_wf_bfb.issubset(other_bfb):
                    winfunc_mode = False
                    break

        if not winfunc_mode:
            return SubqueryType.default, fmask_qfork_bfb_list

        filtered_fmask_qfork_bfb_list: list[FMask_QFork_BFB] = []
        for formula_split_mask, qfork_node, normalized_bfb in fmask_qfork_bfb_list:
            if not inspect_node.qfork_is_window(qfork_node) or normalized_bfb != smallest_wf_bfb:
                # Window function-prioritization is on,
                # so all non-WF q. forks or WF q. forks with non-matching BFBs will be ignored
                continue

            filtered_fmask_qfork_bfb_list.append((formula_split_mask, qfork_node, normalized_bfb))

        return SubqueryType.window_func, filtered_fmask_qfork_bfb_list

    def _unify_masks_for_window_functions(self, split_masks: list[QuerySplitMask]) -> list[QuerySplitMask]:
        """
        Part of the optimization hack for window functions.
        If sub-query masks are in WF mode, then combine them all with the base mask
        so that there are no JOINs. The resulting sub-query's GROUP BY will be eventually removed
        by the mutate_cropped_query method
        """

        subq_types = {mask.subquery_type for mask in split_masks if mask.subquery_type != SubqueryType.generated_base}
        assert len(subq_types) == 1
        subqery_type = next(iter(subq_types))

        if subqery_type != SubqueryType.window_func:
            return split_masks

        base_mask = split_masks[0]
        assert base_mask.is_base
        other_masks = split_masks[1:]

        if len(other_masks) == 0:
            # There's only the base mask
            return split_masks

        if len(other_masks) > 1:
            # Can't optimize for multiple window func subqueries
            return split_masks

        # At this point we must have one base mask and one containing the query fork
        assert len(other_masks) == 1
        other_mask = other_masks[0]

        add_formulas: list[AddFormulaInfo] = []
        add_formula_extracts: set[NodeExtract] = set()
        formula_masks: list[AliasedFormulaSplitMask] = []
        for mask in split_masks:
            for add_formula in mask.add_formulas:
                if add_formula.expr.extract not in add_formula_extracts:
                    add_formulas.append(add_formula)
                    add_formula_extracts.add(add_formula.expr.extract_not_none)
            for formula_mask in mask.formula_split_masks:
                formula_masks.append(formula_mask)

        base_mask = base_mask.clone(
            add_formulas=add_formulas,
            formula_split_masks=formula_masks,
            # replace filters of the generated base query so that we don't ignore BFBs
            filter_indices=other_mask.filter_indices,
        )

        split_masks = [base_mask]
        return split_masks

    def _collect_query_forks(
        self,
        query: CompiledQuery,
        expr_id_gen: PrefixedIdGen,
        disable_grouping_lods: bool = False,
    ) -> list[QueryForkInfo]:
        inspect_env = InspectionEnvironment()

        result: list[QueryForkInfo] = []

        lod_idx_counter = itertools.count()

        def get_new_lod_idx() -> int:
            if disable_grouping_lods:
                # LODs shouldn't be grouped, so generate a new index every time
                return next(lod_idx_counter)
            return 0

        fmask_qfork_list: list[FMask_QFork] = []
        for query_part in self.parent_query_parts_for_splitting:
            formula_list = query.get_formula_list(query_part)
            for formula_idx, formula in enumerate(formula_list):
                fmask_qfork_list += self._get_query_forks_from_expression(
                    node=formula.formula_obj,
                    index_prefix=NodeHierarchyIndex(),
                    query_part=query_part,
                    formula_idx=formula_idx,
                )

        # Normalize BFBs
        available_filter_ids = frozenset(
            filter_formula.original_field_id
            for filter_formula in query.filters
            if filter_formula.original_field_id is not None
        )

        def _normalize_bfb(qfork_node: formula_fork_nodes.QueryFork) -> frozenset[str]:
            return frozenset(qfork_node.before_filter_by.field_names) & available_filter_ids

        fmask_qfork_bfb_list: list[FMask_QFork_BFB] = []
        for formula_split_mask, qfork_node in fmask_qfork_list:
            normalized_bfb = _normalize_bfb(qfork_node)
            fmask_qfork_bfb_list.append((formula_split_mask, qfork_node, normalized_bfb))

        subquery_type, fmask_qfork_bfb_list = self._prioritize_and_filter_query_forks(fmask_qfork_bfb_list)

        qforks_by_signature: OrderedDict[SubqueryForkSignature, QueryForkInfo] = OrderedDict()
        for formula_split_mask, qfork_node, normalized_bfb in fmask_qfork_bfb_list:
            join_type = _JOIN_TYPE_MAP[qfork_node.join_type]
            lod = qfork_node.lod
            dim_list: tuple[formula_nodes.FormulaItem, ...]
            if isinstance(lod, formula_nodes.FixedLodSpecifier):
                dim_list = tuple(lod.dim_list)
            elif isinstance(lod, formula_nodes.InheritedLodSpecifier):
                # Inherit (copy) original query's dimension list
                dim_list = tuple(gb_formula.formula_obj.expr for gb_formula in query.group_by)
            else:
                raise TypeError(f"Unsupported LodSpecifier type: {type(lod).__name__}")

            joining: formula_fork_nodes.QueryForkJoiningBase = qfork_node.joining
            if len(dim_list) == 0:
                # Add a dummy dimension.
                dummy_dim_node = formula_nodes.LiteralInteger.make(value=1)
                dim_list += (dummy_dim_node,)
                joining = formula_fork_nodes.QueryForkJoiningWithList.make(
                    condition_list=[
                        formula_fork_nodes.SelfEqualityJoinCondition.make(expr=dummy_dim_node),
                    ],
                )

            # Generate hashable QueryFork "signature"
            # to deduplicate query forks with the same structure in the same query
            qfork_signature = SubqueryForkSignature(
                dim_extracts=frozenset(dim.extract_not_none for dim in dim_list),
                child_lod_extracts=self._get_child_lod_extracts(node=qfork_node.result_expr),
                join_type=join_type,
                bfb_names=normalized_bfb,
                joining_node_extract=joining.extract_not_none,
                lod_idx=get_new_lod_idx(),
            )

            if qfork_signature not in qforks_by_signature:
                dim_add_formulas: list[AddFormulaInfo] = []
                for dim in dim_list:
                    dim_add_formulas.append(
                        AddFormulaInfo(
                            alias=expr_id_gen.get_id(),
                            expr=dim,
                            from_ids=frozenset(query.joined_from.iter_ids()),
                            is_group_by=not inspect_expression.is_constant_expression(dim),
                        )
                    )

                # Collect non-dimension expressions from the `joining` node.
                # They will be needed to construct JOIN ON conditions correctly
                non_dim_add_formulas: list[AddFormulaInfo] = []
                assert isinstance(joining, formula_fork_nodes.QueryForkJoiningWithList)
                for condition in joining.condition_list:
                    expressions: list[formula_nodes.FormulaItem] = []
                    if isinstance(condition, formula_fork_nodes.SelfEqualityJoinCondition):
                        expressions.append(condition.expr)
                    elif isinstance(condition, formula_fork_nodes.BinaryJoinCondition):
                        expressions.append(condition.expr)
                        expressions.append(condition.fork_expr)
                    else:
                        raise TypeError(type(condition))

                    for expr in expressions:
                        if inspect_expression.is_aggregate_expression(expr, env=inspect_env):
                            non_dim_add_formulas.append(
                                AddFormulaInfo(
                                    alias=expr_id_gen.get_id(),
                                    expr=expr,
                                    from_ids=frozenset(query.joined_from.iter_ids()),
                                    is_group_by=False,
                                )
                            )

                add_formulas = tuple(dim_add_formulas + non_dim_add_formulas)
                bfb_filter_mutations = tuple(
                    ReplacementFormulaMutation(
                        original=mutation.original,
                        replacement=mutation.replacement,
                    )
                    for mutation in qfork_node.bfb_filter_mutations.mutations
                )

                qfork_info = QueryForkInfo(
                    subquery_type=subquery_type,
                    add_formulas=add_formulas,
                    joining_node=joining,
                    join_type=join_type,
                    bfb_field_ids=normalized_bfb,
                    bfb_filter_mutations=bfb_filter_mutations,
                )
                qforks_by_signature[qfork_signature] = qfork_info
                result.append(qfork_info)

            qfork_info = qforks_by_signature[qfork_signature]

            # Deduplicate expressions that are the same
            # (by using the same alias for their split masks)
            subnode_extract = qfork_node.result_expr.extract_not_none
            alias: str
            existing_alias = qfork_info.aliases_by_extract.get(subnode_extract)
            if existing_alias is None:
                alias = expr_id_gen.get_id()
                qfork_info.aliases_by_extract[subnode_extract] = alias
            else:
                alias = existing_alias

            formula_split_mask = AliasedFormulaSplitMask(
                alias=alias,
                query_part=formula_split_mask.query_part,
                formula_list_idx=formula_split_mask.formula_list_idx,
                outer_node_idx=formula_split_mask.outer_node_idx,
                inner_node_idx=formula_split_mask.inner_node_idx,
            )
            qfork_info.formula_split_masks.append(formula_split_mask)

        return result

    def _normalize_joining_node(
        self,
        joining_node: formula_fork_nodes.QueryForkJoiningBase,
        extract: NodeExtract,
        alias: str,
    ) -> formula_fork_nodes.QueryForkJoiningBase:
        def match_func(node: formula_nodes.FormulaItem, *args: Any) -> bool:
            return node.extract == extract

        def replace_func(node: formula_nodes.FormulaItem, *args: Any) -> formula_nodes.FormulaItem:
            return formula_nodes.Field.make(name=alias, meta=node.meta)

        joining_node = joining_node.replace_nodes(match_func=match_func, replace_func=replace_func)
        assert isinstance(joining_node, formula_fork_nodes.QueryForkJoiningBase)
        return joining_node

    def optimize_query_split_masks(self, split_masks: list[QuerySplitMask]) -> list[QuerySplitMask]:
        """Optimize for window function sub-queries"""
        split_masks = self._unify_masks_for_window_functions(split_masks=split_masks)
        return split_masks

    def get_split_masks(
        self,
        query: CompiledQuery,
        expr_id_gen: PrefixedIdGen,
        query_id_gen: PrefixedIdGen,
    ) -> list[QuerySplitMask]:
        mask_list: list[QuerySplitMask] = []
        collected_query_fork_infos = self._collect_query_forks(query, expr_id_gen=expr_id_gen)

        # Filters that have query forks at this level cannot be applied to any other query forks
        # (that would require them to be nested, which would mean that they belong to a different level)
        split_filter_indices = {
            formula_split_mask.formula_list_idx
            for qfork_info in collected_query_fork_infos
            for formula_split_mask in qfork_info.formula_split_masks
            if formula_split_mask.query_part == QueryPart.filters
        }

        for qfork_info in collected_query_fork_infos:
            # Map of expressions to aliases.
            # To replace these expressions with their column aliases
            # in the JOIN ON expressions.
            extract_to_alias_map = {
                add_formula.expr.extract_not_none: add_formula.alias for add_formula in qfork_info.add_formulas
            }

            # Apply this mapping, replacing more complex formulas first.
            # This way if mapping contains nested extracts, i.e. one extract is a child
            # of another, we will replace the parent extract first
            joining_node = qfork_info.joining_node
            for extract, alias in sorted(extract_to_alias_map.items(), key=lambda t: t[0].complexity, reverse=True):
                joining_node = self._normalize_joining_node(joining_node=joining_node, extract=extract, alias=alias)

            add_filters = []

            # Collect indices of filters that should be applied to the sub-query
            filter_indices: set[int] = set()
            for filter_idx, filter_formula in enumerate(query.filters):
                if filter_formula.original_field_id in qfork_info.bfb_field_ids:
                    # Filter field is in BFB, so exclude it unless it is mutated by one of the BFB mutations
                    if any(
                        contains_node(filter_formula.formula_obj, mutation.original)
                        for mutation in qfork_info.bfb_filter_mutations
                    ):
                        new_filter = filter_formula.clone(
                            formula_obj=apply_mutations(filter_formula.formula_obj, qfork_info.bfb_filter_mutations),
                        )
                        add_filters.append(new_filter)
                    continue
                if filter_idx in split_filter_indices:
                    # This filter can only be applied to a higher-level query
                    # Because its pieces are being split of as SELECTs at this level.
                    # Skip it.
                    continue

                filter_indices.add(filter_idx)

            # Put it all together in the sub-query mask
            mask = QuerySplitMask(
                subquery_type=qfork_info.subquery_type,
                subquery_id=query_id_gen.get_id(),
                formula_split_masks=tuple(qfork_info.formula_split_masks),
                filter_indices=frozenset(filter_indices),
                add_filters=tuple(add_filters),
                add_formulas=qfork_info.add_formulas,
                join_type=qfork_info.join_type,
                joining_node=joining_node,
            )
            mask_list.append(mask)

        return mask_list

    def mutate_split_node(self, node: formula_nodes.FormulaItem) -> formula_nodes.FormulaItem:
        if inspect_node.is_aggregate_function(node):
            # Remove all the extra children like LOD, BFB, ID, etc.
            assert isinstance(node, formula_nodes.FuncCall)
            node = formula_nodes.FuncCall.make(
                name=node.name,
                args=node.args,
                meta=node.meta,
            )

        return node

    def mutate_cropped_query(self, query: CompiledQuery) -> CompiledQuery:
        gb_aliases = {gb_formula.not_none_alias for gb_formula in query.group_by}
        inspect_env = InspectionEnvironment()
        agg_statuses: dict[bool, formula_nodes.FormulaItem] = {}  # Save a formula for each status for troubleshooting
        for select_formula in query.select:
            if select_formula.not_none_alias in gb_aliases:
                continue
            if inspect_expression.is_constant_expression(select_formula.formula_obj.expr):
                continue
            is_agg = inspect_expression.is_aggregate_expression(select_formula.formula_obj, env=inspect_env)
            agg_statuses[is_agg] = select_formula.formula_obj.expr

        if len(agg_statuses) == 0:
            # There aren't any non-dimension SELECTs.
            # Nothing to do in this case
            return query

        assert len(agg_statuses) == 1, f"Inconsistent aggregation status among SELECT items. Got: {agg_statuses}"
        agg_status = next(iter(agg_statuses))
        if not agg_status:
            # SELECT items are not aggregated
            # Remove GROUP BY
            query = query.clone(group_by=[])

        return query
