import attr

from dl_formula.inspect.env import InspectionEnvironment
from dl_formula.inspect.expression import is_window_expression
from dl_query_processing.compilation.primitives import (
    CompiledMultiQuery,
    CompiledMultiQueryBase,
    CompiledQuery,
    SubqueryFromObject,
)
from dl_query_processing.enums import ExecutionLevel
from dl_query_processing.multi_query.mutators.base import MultiQueryMutatorBase
from dl_query_processing.multi_query.splitters.mask_based import MultiQuerySplitter
from dl_query_processing.multi_query.splitters.win_func import WinFuncQuerySplitter
from dl_query_processing.multi_query.tools import build_requirement_subtree
from dl_query_processing.utils.name_gen import PrefixedIdGen


@attr.s
class DefaultCompengMultiQueryMutator(MultiQueryMutatorBase):
    """
    Updates `level_type` of non-base sub-queries to `compeng`
    if `multi_query` contains window functions,
    """

    optimize_compeng_usage: bool = attr.ib(kw_only=True, default=True)

    def split_borderline_query(
        self,
        query: CompiledQuery,
        requirement_subtree: CompiledMultiQueryBase,
        query_id_gen: PrefixedIdGen,
        expr_id_gen: PrefixedIdGen,
    ) -> list[CompiledQuery]:
        """
        Split the borderline sub-query into two:
        the top one with the window functions,
        and the bottom one with everything else that the top one selects from
        """

        original_query_id = query.id
        splitter: MultiQuerySplitter = WinFuncQuerySplitter()
        multi_query_patch = splitter.split_query(
            query=query,
            requirement_subtree=requirement_subtree,
            query_id_gen=query_id_gen,
            expr_id_gen=expr_id_gen,
        )
        assert multi_query_patch is not None
        new_subqueries = list(multi_query_patch.patch_multi_query.iter_queries())
        assert len(new_subqueries) == 2

        top_query = next(q for q in new_subqueries if q.id == original_query_id)
        bottom_query = next(q for q in new_subqueries if q.id != original_query_id)
        assert bottom_query.id == top_query.joined_from.root_from_id
        top_query = top_query.clone(level_type=ExecutionLevel.compeng)
        return [top_query, bottom_query]

    def mutate_multi_query(self, multi_query: CompiledMultiQueryBase) -> CompiledMultiQueryBase:
        query_id_gen = PrefixedIdGen("qb")
        expr_id_gen = PrefixedIdGen("eb")

        # First determine if there is a need for compeng
        inspect_env = InspectionEnvironment()
        queries_with_win_funcs: set[str] = set()  # queries that contain WFs or depend on other queries that do
        borderline_queries: set[str] = set()  # queries with WF, whose sub-queries don't have WF
        for query in multi_query.iter_queries():
            query_has_winfuncs = any(
                is_window_expression(node=formula.formula_obj, env=inspect_env) for formula in query.all_formulas
            )
            if query_has_winfuncs:
                queries_with_win_funcs.add(query.id)

        if not queries_with_win_funcs:
            return multi_query

        def propagate_wf_dependency(query: CompiledQuery) -> bool:
            has_wf: bool = False
            if query.id in queries_with_win_funcs:
                has_wf = True

            any_children_with_wf = False
            for child_from in query.joined_from.froms:
                if isinstance(child_from, SubqueryFromObject):
                    child_query_id = child_from.query_id
                    child_has_wf = propagate_wf_dependency(multi_query.get_query_by_id(child_query_id))
                    if child_has_wf:
                        # If a query's child uses window functions, then the query itself uses window functions
                        any_children_with_wf = True
                        has_wf = True
                        queries_with_win_funcs.add(query.id)

            if has_wf and not any_children_with_wf:
                borderline_queries.add(query.id)

            return has_wf

        propagate_wf_dependency(multi_query.get_single_top_query())

        # Replace `level_type` in all of the bottom-level sub-queries
        updated_queries: list[CompiledQuery] = []
        base_from_ids = {from_obj.id for from_obj in multi_query.get_base_froms().values()}
        for query in multi_query.iter_queries():
            query_is_base = bool(set(query.joined_from.iter_ids()) & base_from_ids)
            this_iteration_updated_queries: list[CompiledQuery] = []
            if (
                # optimized version
                self.optimize_compeng_usage
                and query.id in queries_with_win_funcs
                # non-optimized version
                or not self.optimize_compeng_usage
                and not query_is_base
            ):
                if self.optimize_compeng_usage and query.id in borderline_queries:
                    # Split borderline queries so that only the part that starts with window functions
                    # goes to COMPENG. Everything below it should remain in source DB.
                    requirement_subtree = build_requirement_subtree(multi_query, query.id)
                    this_iteration_updated_queries.extend(
                        self.split_borderline_query(
                            query=query,
                            requirement_subtree=requirement_subtree,
                            query_id_gen=query_id_gen,
                            expr_id_gen=expr_id_gen,
                        )
                    )
                else:
                    query = query.clone(level_type=ExecutionLevel.compeng)
                    this_iteration_updated_queries.append(query)

            else:
                this_iteration_updated_queries.append(query)

            updated_queries.extend(this_iteration_updated_queries)

        multi_query = CompiledMultiQuery(queries=updated_queries)
        return multi_query
