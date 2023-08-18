from __future__ import annotations

import bi_formula.core.fork_nodes as fork_nodes
from bi_formula.mutation.lod import ExtAggregationToQueryForkMutation
from bi_formula.mutation.mutation import apply_mutations
from bi_formula.shortcuts import n


def test_lod_aggregation_to_query_fork_mutation():
    mutations = [
        ExtAggregationToQueryForkMutation(global_dimensions=[n.field('f1')]),
    ]

    formula_obj = n.formula(
        n.func.SUM(
            args=[n.func.AVG(args=[], lod=n.include(n.field('f2')))],
            lod=n.include(),
        ),
    )
    formula_obj = apply_mutations(formula_obj, mutations=mutations)
    assert formula_obj == n.formula(
        n.fork(
            join_type=fork_nodes.JoinType.inner,
            result_expr=n.func.SUM(
                args=[
                    n.fork(
                        join_type=fork_nodes.JoinType.inner,
                        result_expr=n.func.AVG(
                            args=[],
                            lod=n.fixed(n.field('f1'), n.field('f2')),
                        ),
                        joining=n.joining(conditions=[
                            n.self_condition(expr=n.field('f1')),
                            n.self_condition(expr=n.field('f2')),
                        ]),
                        lod=n.fixed(n.field('f1'), n.field('f2')),
                    ),
                ],
                lod=n.fixed(n.field('f1')),
            ),
            joining=n.joining(conditions=[
                n.self_condition(expr=n.field('f1')),
            ]),
            lod=n.fixed(n.field('f1')),
        ),
    )
