from __future__ import annotations

import dl_formula.core.fork_nodes as fork_nodes
from dl_formula.mutation.lookup import (
    LookupDefaultBfbMutation,
    LookupFunctionToQueryForkMutation,
)
from dl_formula.mutation.mutation import apply_mutations
from dl_formula.shortcuts import n


def test_ago_to_query_fork_mutation():
    formula_obj = n.formula(n.func.SOMEFUNC(n.func.AGO(n.func.SUM(n.field("f1")), n.field("date"))))
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            LookupFunctionToQueryForkMutation(
                global_dimensions=[
                    n.field("date"),
                    n.field("city"),
                ],
                allow_empty_dimensions=False,
            ),
        ],
    )
    assert formula_obj == n.formula(
        n.func.SOMEFUNC(
            n.fork(
                result_expr=n.func.SUM(n.field("f1")),
                joining=n.joining(
                    conditions=[
                        n.bin_condition(
                            expr=n.field("date"),
                            fork_expr=n.func.DATEADD(n.field("date")),
                        ),
                        n.self_condition(
                            expr=n.field("city"),
                        ),
                    ],
                ),
                lod=n.inherited(),
                bfb_filter_mutations=fork_nodes.BfbFilterMutations.make(
                    fork_nodes.BfbFilterMutation.make(
                        original=n.field("date"),
                        replacement=n.func.DATEADD(n.field("date")),
                    ),
                ),
            )
        ),
    )


def test_lookup_default_bfb_mutation():
    formula_obj = n.formula(n.func.SOMEFUNC(n.func.AGO(n.func.SUM(n.field("f1")), n.field("date"))))
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            LookupDefaultBfbMutation(),
        ],
    )
    assert formula_obj == n.formula(
        n.func.SOMEFUNC(
            n.func.AGO(
                n.func.SUM(n.field("f1")),
                n.field("date"),
                before_filter_by=["date"],
            ),
        )
    )
