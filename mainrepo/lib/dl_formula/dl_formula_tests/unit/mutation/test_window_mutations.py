from __future__ import annotations

from dl_formula.inspect.env import InspectionEnvironment
from dl_formula.mutation.bfb import RemapBfbMutation
from dl_formula.mutation.mutation import apply_mutations
from dl_formula.mutation.window import (
    AmongToWithinGroupingMutation,
    DefaultWindowOrderingMutation,
    IgnoreExtraWithinGroupingMutation,
)
from dl_formula.shortcuts import n


def test_among_to_within_mutation():
    formula_obj = n.formula(n.wfunc.RANK(n.func.SUM(n.field("f3")), among=[n.field("f2")]))
    formula_obj = apply_mutations(
        formula_obj, mutations=[AmongToWithinGroupingMutation(global_dimensions=[n.field("f1"), n.field("f2")])]
    )
    assert formula_obj == n.formula(n.wfunc.RANK(n.func.SUM(n.field("f3")), within=[n.field("f1")]))


def test_ignore_extra_within_mutation():
    inspect_env = InspectionEnvironment()
    formula_obj = n.formula(n.wfunc.RANK(n.func.SUM(n.field("f1")), within=[n.field("f2"), n.field("f3")]))
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[IgnoreExtraWithinGroupingMutation(global_dimensions=[n.field("f2")], inspect_env=inspect_env)],
    )
    assert formula_obj == n.formula(n.wfunc.RANK(n.func.SUM(n.field("f1")), within=[n.field("f2")]))


def test_default_window_ordering_mutation_basic():
    formula_obj = n.formula(n.wfunc.RSUM(n.func.SUM(n.field("f3")), total=True))  # RSUM requires default ordering
    formula_obj = apply_mutations(
        formula_obj, mutations=[DefaultWindowOrderingMutation(default_order_by=[n.field("f1"), n.field("f2")])]
    )
    assert formula_obj == n.formula(
        n.wfunc.RSUM(n.func.SUM(n.field("f3")), total=True, order_by=[n.field("f1"), n.field("f2")])
    )


def test_default_window_ordering_mutation_with_order_by_override():
    formula_obj = n.formula(
        n.wfunc.RSUM(n.func.SUM(n.field("f3")), total=True, order_by=[n.desc(n.field("f2")), n.field("f3")])
    )
    formula_obj = apply_mutations(
        formula_obj, mutations=[DefaultWindowOrderingMutation(default_order_by=[n.field("f1"), n.field("f2")])]
    )
    assert formula_obj == n.formula(
        n.wfunc.RSUM(
            n.func.SUM(n.field("f3")),
            total=True,
            order_by=[
                n.desc(n.field("f2")),
                n.field("f3"),
                n.field("f1"),
            ],
        ),
    )


def test_remap_before_filter_by_mutation():
    formula_obj = n.formula(
        n.wfunc.FUNC(
            before_filter_by=(),
            args=[
                n.wfunc.FUNC(
                    before_filter_by=("A",),
                    args=[
                        n.func.FUNC(before_filter_by=("Q", "R"), args=[]),
                    ],
                ),
            ],
        ),
    )
    formula_obj = apply_mutations(
        formula_obj, mutations=[RemapBfbMutation(name_mapping={"A": "a1", "Q": "a2", "R": "a3"})]
    )
    assert formula_obj == n.formula(
        n.wfunc.FUNC(
            before_filter_by=(),
            args=[
                n.wfunc.FUNC(
                    before_filter_by=("a1",),
                    args=[
                        n.func.FUNC(before_filter_by=("a2", "a3"), args=[]),
                    ],
                ),
            ],
        ),
    )
