from __future__ import annotations

from bi_formula.inspect.env import InspectionEnvironment
from bi_formula.mutation.bfb import (
    NormalizeBeforeFilterByMutation,
    RemapBfbMutation,
)
from bi_formula.mutation.mutation import apply_mutations
from bi_formula.mutation.tagging import FunctionLevelTagMutation
from bi_formula.mutation.window import (
    AmongToWithinGroupingMutation,
    DefaultWindowOrderingMutation,
    IgnoreExtraWithinGroupingMutation,
)
from bi_formula.shortcuts import n


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


def test_normalize_before_filter_by_mutation():
    formula_obj = n.formula(
        n.wfunc.RSUM(
            before_filter_by=(),
            args=[
                n.wfunc.RSUM(
                    before_filter_by=("A",),
                    args=[
                        n.wfunc.RSUM(
                            before_filter_by=(),
                            args=[
                                n.wfunc.RSUM(
                                    before_filter_by=("A", "B"),
                                    args=[
                                        n.wfunc.RSUM(
                                            before_filter_by=(),
                                            args=[
                                                n.wfunc.RSUM(
                                                    before_filter_by=("A", "B", "C", "D"),
                                                    args=[
                                                        n.wfunc.RSUM(before_filter_by=(), args=[n.func.NON_WIN_FUNC()]),
                                                    ],
                                                ),
                                            ],
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        ),
    )
    formula_obj = apply_mutations(
        formula_obj, mutations=[NormalizeBeforeFilterByMutation(available_filter_field_ids=("A", "C", "D"))]
    )
    assert formula_obj == n.formula(
        n.wfunc.RSUM(
            before_filter_by=(),
            args=[
                n.wfunc.RSUM(
                    before_filter_by=("A",),
                    args=[
                        n.wfunc.RSUM(
                            before_filter_by=("A",),
                            args=[
                                n.wfunc.RSUM(
                                    before_filter_by=("A",),
                                    args=[
                                        n.wfunc.RSUM(
                                            before_filter_by=("A",),
                                            args=[
                                                n.wfunc.RSUM(
                                                    before_filter_by=("A", "C", "D"),
                                                    args=[
                                                        n.wfunc.RSUM(
                                                            before_filter_by=("A", "C", "D"),
                                                            args=[n.func.NON_WIN_FUNC()],
                                                        ),
                                                    ],
                                                ),
                                            ],
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        ),
    )


def test_function_level_tag_mutation():
    formula_obj = n.formula(
        n.wfunc.RANK(
            before_filter_by=(),
            args=[
                n.wfunc.RANK(
                    before_filter_by=("A",),
                    args=[
                        n.wfunc.RANK(
                            before_filter_by=("A",),
                            args=[
                                n.wfunc.RANK(
                                    before_filter_by=("A",),
                                    args=[
                                        n.wfunc.RANK(
                                            before_filter_by=("A", "B"),
                                            args=[
                                                n.wfunc.RANK(
                                                    before_filter_by=("A", "B"),
                                                    args=[
                                                        n.wfunc.RANK(
                                                            before_filter_by=("A", "B", "C", "D"),
                                                            args=[n.func.NON_WIN_FUNC()],
                                                        ),
                                                    ],
                                                ),
                                            ],
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        ),
    )
    formula_obj = apply_mutations(formula_obj, mutations=[FunctionLevelTagMutation()])
    assert formula_obj == n.formula(
        n.tagged(
            tag=n.level_tag(frozenset(), 0),
            expr=n.p(
                n.wfunc.RANK(
                    before_filter_by=(),
                    args=[
                        n.tagged(
                            tag=n.level_tag(frozenset(("A",)), -2),
                            expr=n.p(
                                n.wfunc.RANK(
                                    before_filter_by=("A",),
                                    args=[
                                        n.tagged(
                                            tag=n.level_tag(frozenset(("A",)), -1),
                                            expr=n.p(
                                                n.wfunc.RANK(
                                                    before_filter_by=("A",),
                                                    args=[
                                                        n.tagged(
                                                            tag=n.level_tag(frozenset(("A",)), 0),
                                                            expr=n.p(
                                                                n.wfunc.RANK(
                                                                    before_filter_by=("A",),
                                                                    args=[
                                                                        n.tagged(
                                                                            tag=n.level_tag(frozenset(("A", "B")), -1),
                                                                            expr=n.p(
                                                                                n.wfunc.RANK(
                                                                                    before_filter_by=("A", "B"),
                                                                                    args=[
                                                                                        n.tagged(
                                                                                            tag=n.level_tag(
                                                                                                frozenset(("A", "B")), 0
                                                                                            ),
                                                                                            expr=n.p(
                                                                                                n.wfunc.RANK(
                                                                                                    before_filter_by=(
                                                                                                        "A",
                                                                                                        "B",
                                                                                                    ),
                                                                                                    args=[
                                                                                                        n.tagged(
                                                                                                            tag=n.level_tag(
                                                                                                                frozenset(
                                                                                                                    (
                                                                                                                        "A",
                                                                                                                        "B",
                                                                                                                        "C",
                                                                                                                        "D",
                                                                                                                    )
                                                                                                                ),
                                                                                                                0,
                                                                                                            ),
                                                                                                            expr=n.p(
                                                                                                                n.wfunc.RANK(
                                                                                                                    before_filter_by=(
                                                                                                                        "A",
                                                                                                                        "B",
                                                                                                                        "C",
                                                                                                                        "D",
                                                                                                                    ),
                                                                                                                    args=[
                                                                                                                        n.func.NON_WIN_FUNC()
                                                                                                                    ],
                                                                                                                )
                                                                                                            ),
                                                                                                        ),
                                                                                                    ],
                                                                                                )
                                                                                            ),
                                                                                        ),
                                                                                    ],
                                                                                )
                                                                            ),
                                                                        ),
                                                                    ],
                                                                )
                                                            ),
                                                        ),
                                                    ],
                                                )
                                            ),
                                        ),
                                    ],
                                )
                            ),
                        ),
                    ],
                )
            ),
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
