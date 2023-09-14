from __future__ import annotations

import datetime
import uuid

from bi_formula.core import nodes
from bi_formula.shortcuts import n


def test_literal_shortcuts():
    assert n.lit("qwerty") == nodes.LiteralString.make("qwerty")
    assert n.lit(123) == nodes.LiteralInteger.make(123)
    assert n.lit(12.3) == nodes.LiteralFloat.make(12.3)
    assert n.lit(True) == nodes.LiteralBoolean.make(True)
    assert n.lit(None) == nodes.Null()
    dt = datetime.datetime.utcnow().replace(microsecond=0, tzinfo=None)
    assert n.lit(dt) == nodes.LiteralGenericDatetime.make(dt)
    assert n.lit(dt.date()) == nodes.LiteralDate.make(dt.date())
    my_uuid = uuid.uuid4()
    assert n.lit(my_uuid) == nodes.LiteralUuid.make(my_uuid)


def test_functions():
    assert n.func("+")(n.func.MY_FUNC(123, True, nodes.LiteralString.make("456")), None) == (
        nodes.FuncCall.make(
            name="+",
            args=[
                nodes.FuncCall.make(
                    name="my_func",
                    args=[
                        nodes.LiteralInteger.make(123),
                        nodes.LiteralBoolean.make(True),
                        nodes.LiteralString.make("456"),
                    ],
                ),
                nodes.Null(),
            ],
        )
    )


def test_window_functions():
    assert n.func.MY_FUNC(n.field("f0"), total=True) == (
        nodes.WindowFuncCall.make(
            name="my_func", args=[nodes.Field.make(name="f0")], grouping=nodes.WindowGroupingTotal()
        )
    )
    assert n.func.MY_FUNC(n.field("f0"), within=[n.field("f1")]) == (
        nodes.WindowFuncCall.make(
            name="my_func",
            args=[nodes.Field.make(name="f0")],
            grouping=nodes.WindowGroupingWithin.make(dim_list=[nodes.Field.make(name="f1")]),
        )
    )
    assert n.func.MY_FUNC(n.field("f0"), among=[n.field("f1")]) == (
        nodes.WindowFuncCall.make(
            name="my_func",
            args=[nodes.Field.make(name="f0")],
            grouping=nodes.WindowGroupingAmong.make(dim_list=[nodes.Field.make(name="f1")]),
        )
    )
    assert n.func.MY_FUNC(n.field("f0"), grouping=n.among(n.field("f1"))) == (
        nodes.WindowFuncCall.make(
            name="my_func",
            args=[nodes.Field.make(name="f0")],
            grouping=nodes.WindowGroupingAmong.make(dim_list=[nodes.Field.make(name="f1")]),
        )
    )
    assert n.wfunc.MY_FUNC(n.field("f0")) == (
        nodes.WindowFuncCall.make(
            name="my_func", args=[nodes.Field.make(name="f0")], grouping=nodes.WindowGroupingTotal()
        )
    )


def test_window_functions_order_by():
    assert n.func.MY_FUNC(n.field("f0"), total=True, order_by=[n.field("f1")]) == (
        nodes.WindowFuncCall.make(
            name="my_func",
            args=[nodes.Field.make(name="f0")],
            ordering=nodes.Ordering.make(
                expr_list=[
                    nodes.Field.make("f1"),
                ]
            ),
        )
    )


def test_window_functions_before_filter_by():
    assert n.func.MY_FUNC(n.field("f0"), total=True, before_filter_by=[n.field("f1")]) == (
        nodes.WindowFuncCall.make(
            name="my_func",
            args=[nodes.Field.make(name="f0")],
            before_filter_by=nodes.BeforeFilterBy.make(
                field_names=frozenset(
                    [
                        "f1",
                    ]
                )
            ),
        )
    )


def test_logical_blocks():
    assert n.if_(n.func.WHATEVER()).then("qwerty").else_("uiop") == (
        nodes.IfBlock.make(
            if_list=[
                nodes.IfPart.make(
                    cond=nodes.FuncCall.make(name="whatever", args=[]),
                    expr=nodes.LiteralString.make("qwerty"),
                ),
            ],
            else_expr=nodes.LiteralString.make("uiop"),
        )
    )
    assert n.if_(
        n.if_(n.func.WHATEVER()).then("qwerty"),
        n.if_(n.func.WHENEVER()).then("uiop"),
        n.if_(n.func.WHOEVER()).then("asdfg"),
    ).else_("xyz") == (
        nodes.IfBlock.make(
            if_list=[
                nodes.IfPart.make(
                    cond=nodes.FuncCall.make(name="whatever", args=[]),
                    expr=nodes.LiteralString.make("qwerty"),
                ),
                nodes.IfPart.make(
                    cond=nodes.FuncCall.make(name="whenever", args=[]),
                    expr=nodes.LiteralString.make("uiop"),
                ),
                nodes.IfPart.make(
                    cond=nodes.FuncCall.make(name="whoever", args=[]),
                    expr=nodes.LiteralString.make("asdfg"),
                ),
            ],
            else_expr=nodes.LiteralString.make("xyz"),
        )
    )
    assert n.case(n.field("my_field")).whens(
        n.when(n.func.WHATEVER()).then("qwerty"),
        n.when(n.func.WHENEVER()).then("uiop"),
    ).else_("xyz") == (
        nodes.CaseBlock.make(
            case_expr=nodes.Field.make(name="my_field"),
            when_list=[
                nodes.WhenPart.make(
                    val=nodes.FuncCall.make(name="whatever", args=[]),
                    expr=nodes.LiteralString.make("qwerty"),
                ),
                nodes.WhenPart.make(
                    val=nodes.FuncCall.make(name="whenever", args=[]),
                    expr=nodes.LiteralString.make("uiop"),
                ),
            ],
            else_expr=nodes.LiteralString.make("xyz"),
        )
    )
