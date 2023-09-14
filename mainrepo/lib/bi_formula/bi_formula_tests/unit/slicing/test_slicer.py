from __future__ import annotations

from collections import defaultdict

from bi_formula.core import nodes
from bi_formula.core.tag import LevelTag
from bi_formula.shortcuts import n
from bi_formula.slicing.schema import (
    AggregateFunctionLevelBoundary,
    NestedLevelTaggedBoundary,
    NonFieldsBoundary,
    SliceSchema,
    TopLevelBoundary,
    WindowFunctionLevelBoundary,
)
from bi_formula.slicing.slicer import (
    FormulaSlicer,
    LevelSlice,
    SlicedFormulaInfo,
)


class NameGen:
    """
    Minor helper for numbered field names.

    >>> ng = NameGen()
    >>> ng(None, 'fld1')
    'fld1_1'
    >>> ng(None, 'fld2')
    'fld2_1'
    >>> ng(None, 'fld1')
    'fld1_2'
    >>> NameGen()(None, 'fld2')
    'fld2_1'
    """

    def __init__(self):
        self.counters = defaultdict(lambda: 0)

    def __call__(self, node: nodes.FormulaItem, name: str) -> str:
        self.counters[name] += 1
        return f"{name}_{self.counters[name]}"


def test_aggregation_function_slicer():
    name_gen = NameGen()

    # slice aggregations
    agg_slicer = FormulaSlicer(
        slice_schema=SliceSchema(
            levels=[
                AggregateFunctionLevelBoundary(name="agg", name_gen=name_gen),
                TopLevelBoundary(name="top", name_gen=name_gen),
            ]
        ),
    )

    sliced_fla_info = agg_slicer.slice_formula(
        formula=n.formula(n.func.SUM(n.func.MY_FUNC(n.field("My Field")))),
    )
    expected = SlicedFormulaInfo(
        slices=[
            LevelSlice(
                name="agg",
                required_fields=set(),
                aliased_nodes={"agg_1": n.formula(n.func.MY_FUNC(n.field("My Field")))},
            ),
            LevelSlice(
                name="top",
                required_fields={"agg_1"},
                aliased_nodes={"top_1": n.formula(n.func.SUM(n.field("agg_1")))},
            ),
        ]
    )
    assert sliced_fla_info == expected

    # check laziness of slices
    assert not sliced_fla_info.slices[0].is_lazy()
    assert not sliced_fla_info.slices[1].is_lazy()

    # no aggregation, so the top slice should be lazy
    sliced_fla_info = agg_slicer.slice_formula(
        formula=n.formula(n.func.OTHER_FUNC(n.func.MY_FUNC(n.field("My Field")))),
    )
    assert len(sliced_fla_info.slices) == 2
    # check laziness of slices
    assert not sliced_fla_info.slices[0].is_lazy()
    assert sliced_fla_info.slices[1].is_lazy()


def test_window_function_slicer():
    name_gen = NameGen()

    win_slicer = FormulaSlicer(
        slice_schema=SliceSchema(
            levels=[
                WindowFunctionLevelBoundary(name="win", name_gen=name_gen),
                TopLevelBoundary(name="top", name_gen=name_gen),
            ]
        ),
    )

    sliced_fla_info = win_slicer.slice_formula(
        formula=n.formula(
            n.func("*")(
                n.func("/")(
                    n.wfunc.RSUM(
                        n.func.SUM(n.func.MY_FUNC(n.field("Sales"))),
                        grouping=n.among(n.field("Dim")),
                        order_by=[n.desc(n.field("Order Date"))],
                    ),
                    n.wfunc.SUM(n.func.SUM(n.func.MY_FUNC(n.field("Sales"))), grouping=n.total()),
                ),
                n.lit(100),
            )
        ),
    )
    expected = SlicedFormulaInfo(
        slices=[
            LevelSlice(
                name="win",
                required_fields=set(),
                aliased_nodes={
                    "win_1": n.formula(n.func.SUM(n.func.MY_FUNC(n.field("Sales")))),
                    "win_2": n.formula(n.field("Order Date")),
                    "win_3": n.formula(n.field("Dim")),
                },
            ),
            LevelSlice(
                name="top",
                required_fields={"win_1", "win_2", "win_3"},
                aliased_nodes={
                    "top_1": n.formula(
                        n.func("*")(
                            n.func("/")(
                                n.wfunc.RSUM(
                                    n.field("win_1"),
                                    grouping=n.among(n.field("win_3")),
                                    order_by=[n.desc(n.field("win_2"))],
                                ),
                                n.wfunc.SUM(n.field("win_1"), grouping=n.total()),
                            ),
                            n.lit(100),
                        )
                    )
                },
            ),
        ]
    )
    assert sliced_fla_info == expected

    # check laziness of slices
    assert not sliced_fla_info.slices[0].is_lazy()
    assert not sliced_fla_info.slices[1].is_lazy()

    # no window function, so the top slice should be lazy
    sliced_fla_info = win_slicer.slice_formula(
        formula=n.formula(n.func.OTHER_FUNC(n.func.MY_FUNC(n.field("My Field")))),
    )
    assert len(sliced_fla_info.slices) == 2
    # check laziness of slices
    assert not sliced_fla_info.slices[0].is_lazy()
    assert sliced_fla_info.slices[1].is_lazy()


def test_non_fields_slicer():
    name_gen = NameGen()

    slicer = FormulaSlicer(
        slice_schema=SliceSchema(
            levels=[
                NonFieldsBoundary(name="nf", name_gen=name_gen),
                TopLevelBoundary(name="top", name_gen=name_gen),
            ]
        ),
    )

    sliced_fla_info = slicer.slice_formula(
        formula=n.formula(
            n.func("*")(
                n.func("/")(
                    n.wfunc.RSUM(
                        n.func.SUM(n.func.MY_FUNC(n.field("Sales"))),
                        grouping=n.among(n.field("Dim")),
                        order_by=[n.field("Order Date")],
                    ),
                    n.wfunc.SUM(n.func.SUM(n.func.MY_FUNC(n.field("Sales"))), grouping=n.total()),
                ),
                n.lit(100),
            )
        ),
    )
    expected = SlicedFormulaInfo(
        slices=[
            LevelSlice(
                name="nf",
                required_fields=set(),
                aliased_nodes={
                    "nf_1": n.formula(n.field("Sales")),
                    "nf_2": n.formula(n.field("Order Date")),
                    "nf_3": n.formula(n.field("Dim")),
                },
            ),
            LevelSlice(
                name="top",
                required_fields={"nf_1", "nf_2", "nf_3"},
                aliased_nodes={
                    "top_1": n.formula(
                        n.func("*")(
                            n.func("/")(
                                n.wfunc.RSUM(
                                    n.func.SUM(n.func.MY_FUNC(n.field("nf_1"))),
                                    grouping=n.among(n.field("nf_3")),
                                    order_by=[n.field("nf_2")],
                                ),
                                n.wfunc.SUM(
                                    n.func.SUM(n.func.MY_FUNC(n.field("nf_1"))),
                                    grouping=n.total(),
                                ),
                            ),
                            n.lit(100),
                        )
                    ),
                },
            ),
        ]
    )
    assert sliced_fla_info == expected

    # check laziness of slices
    assert sliced_fla_info.slices[0].is_lazy()
    assert not sliced_fla_info.slices[1].is_lazy()

    # nothing except field, so both slices should be lazy
    sliced_fla_info = slicer.slice_formula(
        formula=n.formula(n.field("My Field")),
    )
    assert len(sliced_fla_info.slices) == 2
    # check laziness of slices
    assert sliced_fla_info.slices[0].is_lazy()
    assert sliced_fla_info.slices[1].is_lazy()


def test_tagged_slicer():
    name_gen = NameGen()

    slicer = FormulaSlicer(
        slice_schema=SliceSchema(
            levels=[
                NestedLevelTaggedBoundary(
                    name="tagged",
                    name_gen=name_gen,
                    tag=LevelTag(bfb_names=frozenset({"slice_here"}), func_nesting=-1, qfork_nesting=0),
                ),
                TopLevelBoundary(name="top", name_gen=name_gen),
            ]
        ),
    )

    sliced_fla_info = slicer.slice_formula(
        formula=n.formula(
            n.func.SUM(
                n.tagged(
                    tag=LevelTag(bfb_names=frozenset({"slice_here"}), func_nesting=-1, qfork_nesting=0),
                    expr=n.p(n.wfunc.RSUM(n.field("My Field"))),
                )
            ),
        ),
    )
    expected = SlicedFormulaInfo(
        slices=[
            LevelSlice(
                name="tagged",
                required_fields=set(),
                aliased_nodes={"tagged_1": n.formula(n.wfunc.RSUM(n.field("My Field")))},
            ),
            LevelSlice(
                name="top",
                required_fields={"tagged_1"},
                aliased_nodes={
                    "top_1": n.formula(
                        n.func.SUM(
                            n.tagged(
                                tag=LevelTag(bfb_names=frozenset({"slice_here"}), func_nesting=-1, qfork_nesting=0),
                                expr=n.p(n.field("tagged_1")),
                            )
                        )
                    )
                },
            ),
        ]
    )
    assert sliced_fla_info == expected
