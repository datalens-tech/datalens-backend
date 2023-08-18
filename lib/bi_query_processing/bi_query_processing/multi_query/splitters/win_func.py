import attr

from bi_constants.enums import JoinType

import bi_formula.core.nodes as formula_nodes
import bi_formula.core.fork_nodes as formula_fork_nodes
from bi_formula.core.index import NodeHierarchyIndex
import bi_formula.inspect.node as inspect_node
import bi_formula.inspect.expression as inspect_expression
from bi_formula.inspect.env import InspectionEnvironment

from bi_query_processing.compilation.primitives import CompiledQuery
from bi_query_processing.multi_query.splitters.mask_based import (
    MultiQuerySplitter, AliasedFormulaSplitMask, QuerySplitMask, SubqueryType
)
from bi_query_processing.utils.name_gen import PrefixedIdGen


@attr.s
class WinFuncQuerySplitter(MultiQuerySplitter):
    """
    Splitter that splits a query into two parts:
    one with window functions, and the other with everything else.
    All window functions are expected to be the topmost node in formulas.
    This is the way `QueryForkQuerySplitter` leaves them.
    """

    def _collect_win_func_split_indices_from_expression(
            self, node: formula_nodes.Formula, inspect_env: InspectionEnvironment,
    ) -> list[NodeHierarchyIndex]:
        """
        Collect indices of nodes that have to be split off.
        If the formula has a WF as its topmost node, then there will be an index for each of its children.
        Otherwise an index pointing to the whole expression is returned.
        """

        result: list[NodeHierarchyIndex] = []

        expr = node.expr
        expr_index = NodeHierarchyIndex(indices=(0,))
        if inspect_node.is_window_function(expr):
            for child_index, child_node, _ in inspect_expression.enumerate_autonomous_children(expr, prefix=expr_index):
                if inspect_expression.is_constant_expression(child_node, env=inspect_env):
                    # Ignore constants
                    continue

                assert not inspect_expression.is_window_expression(child_node, env=inspect_env)
                result.append(child_index)

        else:
            result.append(expr_index)
            assert not inspect_expression.is_window_expression(expr, env=inspect_env)

        return result

    def _collect_win_func_formula_masks(
            self, query: CompiledQuery, expr_id_gen: PrefixedIdGen,
    ) -> list[AliasedFormulaSplitMask]:
        """
        Iterate over all formulas in the query and collect indices of nodes that should be split off.
        Generate an `AliasedFormulaSplitMask` for each.
        """

        inspect_env = InspectionEnvironment()
        result: list[AliasedFormulaSplitMask] = []
        for query_part in self.parent_query_parts_for_splitting:
            formula_list = query.get_formula_list(query_part)
            for formula_idx, formula in enumerate(formula_list):
                indices = self._collect_win_func_split_indices_from_expression(
                    node=formula.formula_obj, inspect_env=inspect_env,
                )
                result += [
                    AliasedFormulaSplitMask(
                        inner_node_idx=node_idx,
                        outer_node_idx=node_idx,
                        query_part=query_part,
                        formula_list_idx=formula_idx,
                        alias=expr_id_gen.get_id(),
                    )
                    for node_idx in indices
                ]

        return result

    def get_split_masks(
            self, query: CompiledQuery, expr_id_gen: PrefixedIdGen, query_id_gen: PrefixedIdGen,
    ) -> list[QuerySplitMask]:
        """
        Collect formula split masks for all formulas in the query and generate a single query split mask
        with all the formula masks in it.
        """

        formula_split_masks = self._collect_win_func_formula_masks(query, expr_id_gen=expr_id_gen)

        # Put it all together in the sub-query mask
        subquery_mask = QuerySplitMask(
            subquery_type=SubqueryType.default,
            subquery_id=query_id_gen.get_id(),
            formula_split_masks=tuple(formula_split_masks),
            filter_indices=frozenset(),
            add_formulas=(),
            join_type=JoinType.inner,
            joining_node=formula_fork_nodes.QueryForkJoiningWithList.make(condition_list=[]),
        )
        return [subquery_mask]
