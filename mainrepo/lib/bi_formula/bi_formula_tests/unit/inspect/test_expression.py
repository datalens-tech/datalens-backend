from __future__ import annotations

import copy

from bi_formula.collections import NodeSet
from bi_formula.core import nodes
from bi_formula.core.datatype import DataType
from bi_formula.core.index import NodeHierarchyIndex
from bi_formula.inspect.env import InspectionEnvironment
from bi_formula.inspect.expression import (
    enumerate_fields,
    get_query_fork_nesting_level,
    infer_data_type,
    is_aggregate_expression,
    is_aggregated_above_sub_node,
    is_bound_only_to,
    is_constant_expression,
    is_query_fork_expression,
    is_window_expression,
    iter_operation_calls,
    used_fields,
    used_func_calls,
)
from bi_formula.shortcuts import n


def test_used_func_calls():
    assert used_func_calls(n.formula(nodes.FuncCall.make(name="func", args=[nodes.LiteralString.make("qwe")]))) == [
        nodes.FuncCall.make(name="func", args=[nodes.LiteralString.make("qwe")]),
    ]
    assert used_func_calls(
        n.formula(
            nodes.FuncCall.make(
                name="func",
                args=[
                    nodes.Field.make("f1"),
                    nodes.FuncCall.make(
                        name="other_func",
                        args=[
                            nodes.Field.make("f2"),
                            n.p(nodes.Field.make("f3")),
                        ],
                    ),
                ],
            )
        )
    ) == [
        nodes.FuncCall.make(
            name="func",
            args=[
                nodes.Field.make("f1"),
                nodes.FuncCall.make(
                    name="other_func",
                    args=[
                        nodes.Field.make("f2"),
                        n.p(nodes.Field.make("f3")),
                    ],
                ),
            ],
        ),
        nodes.FuncCall.make(
            name="other_func",
            args=[
                nodes.Field.make("f2"),
                n.p(nodes.Field.make("f3")),
            ],
        ),
    ]


def test_used_fields():
    assert used_fields(n.formula(nodes.Field.make("f1"))) == [
        nodes.Field.make("f1"),
    ]
    assert used_fields(
        n.formula(
            nodes.FuncCall.make(
                name="func",
                args=[
                    nodes.Field.make("f1"),
                    nodes.FuncCall.make(
                        name="other_func",
                        args=[
                            nodes.Field.make("f2"),
                            n.p(nodes.Field.make("f3")),
                        ],
                    ),
                ],
            )
        )
    ) == [
        nodes.Field.make("f1"),
        nodes.Field.make("f2"),
        nodes.Field.make("f3"),
    ]


def test_is_constant_expression():
    env = InspectionEnvironment()
    assert is_constant_expression(nodes.LiteralString.make("qwe"), env=env)
    assert is_constant_expression(nodes.Null(), env=env)
    assert not is_constant_expression(nodes.FuncCall.make(name="func", args=[nodes.LiteralString.make("qwe")]), env=env)
    assert is_constant_expression(n.p(nodes.LiteralString.make("qwe")), env=env)
    assert not is_constant_expression(nodes.FuncCall.make(name="func", args=[nodes.LiteralString.make("qwe")]), env=env)
    assert not is_constant_expression(
        n.p(nodes.FuncCall.make(name="func", args=[nodes.LiteralString.make("qwe")])), env=env
    )


def test_is_is_bound_only_to():
    dim_1 = nodes.Field.make("Dim Field")
    dim_2 = nodes.FuncCall.make(
        name="+",
        args=[
            nodes.Field.make("Other Field"),
            nodes.FuncCall.make(
                name="dim_func",
                args=[
                    nodes.Field.make("Third Field"),
                ],
            ),
        ],
    )
    allow_nodes = NodeSet([dim_1, dim_2])
    assert is_bound_only_to(nodes.LiteralString.make("qwe"), allow_nodes=allow_nodes)
    assert is_bound_only_to(n.p(nodes.LiteralString.make("qwe")), allow_nodes=allow_nodes)
    assert is_bound_only_to(n.p(dim_1), allow_nodes=allow_nodes)
    assert is_bound_only_to(n.p(dim_2), allow_nodes=allow_nodes)
    assert is_bound_only_to(
        nodes.FuncCall.make(name="func", args=[nodes.LiteralString.make("qwe")]), allow_nodes=allow_nodes
    )
    assert is_bound_only_to(
        n.p(nodes.FuncCall.make(name="func", args=[nodes.LiteralString.make("qwe"), dim_1])),
        allow_nodes=allow_nodes,
    )
    assert is_bound_only_to(
        nodes.FuncCall.make(
            name="func",
            args=[
                nodes.LiteralString.make("qwe"),
                n.p(nodes.LiteralString.make("qwe")),
                dim_2,
            ],
        ),
        allow_nodes=allow_nodes,
    )
    assert not is_bound_only_to(nodes.Field.make(name="my field"), allow_nodes=allow_nodes)
    assert not is_bound_only_to(n.p(nodes.Field.make(name="my field")), allow_nodes=allow_nodes)
    assert not is_bound_only_to(
        nodes.FuncCall.make(
            name="func",
            args=[
                nodes.LiteralString.make("qwe"),
                nodes.Field.make(name="my field"),
                dim_2,
            ],
        ),
        allow_nodes=allow_nodes,
    )


def test_is_aggregate_expression():
    env = InspectionEnvironment()
    assert is_aggregate_expression(nodes.FuncCall.make(name="avg", args=[nodes.LiteralString.make("qwe")]), env=env)
    assert is_aggregate_expression(nodes.FuncCall.make(name="avg", args=[nodes.Field.make("my field")]), env=env)
    assert is_aggregate_expression(
        n.p(nodes.FuncCall.make(name="avg", args=[nodes.LiteralString.make("qwe")])), env=env
    )
    assert not is_aggregate_expression(nodes.LiteralString.make("qwe"), env=env)
    assert not is_aggregate_expression(n.p(nodes.LiteralString.make("qwe")), env=env)
    assert not is_aggregate_expression(
        nodes.FuncCall.make(name="func", args=[nodes.LiteralString.make("qwe")]), env=env
    )
    assert not is_aggregate_expression(
        nodes.WindowFuncCall.make(
            name="sum", args=[nodes.Field.make("my field")], grouping=nodes.WindowGroupingTotal()
        ),
        env=env,
    )


def test_is_aggregated_above_sub_node():
    sub_node = n.field("My Field")
    # Simple aggregation
    assert is_aggregated_above_sub_node(
        node=n.func.MAX(sub_node),
        sub_node=sub_node,
    )
    # Aggregation with custom LOD
    assert is_aggregated_above_sub_node(
        node=n.func.MAX(sub_node, lod=n.fixed()),
        sub_node=sub_node,
    )
    # Aggregation + wrapper
    assert is_aggregated_above_sub_node(
        node=n.func.MAX(n.func.CONCAT(sub_node)),
        sub_node=sub_node,
    )
    # Wrapper + aggregation + wrapper
    assert is_aggregated_above_sub_node(
        node=n.func.FLOOR(n.func.MAX(n.func.CONCAT(sub_node))),
        sub_node=sub_node,
    )
    # Same, but by index
    assert is_aggregated_above_sub_node(
        node=n.func.FLOOR(n.func.MAX(n.func.CONCAT(sub_node))),
        index=NodeHierarchyIndex.make(0, 0, 0),
    )
    # No aggregations
    assert not is_aggregated_above_sub_node(
        node=n.func.FLOOR(n.func.CONCAT(sub_node)),
        sub_node=sub_node,
    )
    # Same, but by index
    assert not is_aggregated_above_sub_node(
        node=n.func.FLOOR(n.func.CONCAT(sub_node)),
        index=NodeHierarchyIndex.make(0, 0),
    )
    # Window function in this case, not agg
    assert not is_aggregated_above_sub_node(
        node=n.func.FLOOR(n.wfunc.MAX(n.func.CONCAT(sub_node))),
        sub_node=sub_node,
    )
    # Formula contains an aggregation, but it doesn't wrap sub_node
    assert not is_aggregated_above_sub_node(
        node=n.func.CONCAT(sub_node, n.func.MAX(copy.copy(sub_node))),
        sub_node=sub_node,
    )


def test_is_window_expression():
    env = InspectionEnvironment()
    assert is_window_expression(
        nodes.WindowFuncCall.make(
            name="rank",
            args=[nodes.LiteralString.make("qwe")],
            grouping=nodes.WindowGroupingTotal(),
        ),
        env=env,
    )
    assert is_window_expression(
        nodes.FuncCall.make(
            name="sum",
            args=[
                nodes.WindowFuncCall.make(
                    name="rank",
                    args=[nodes.Field.make("my field")],
                    grouping=nodes.WindowGroupingTotal(),
                )
            ],
        ),
        env=env,
    )
    assert is_window_expression(
        n.p(nodes.WindowFuncCall.make(name="rank", args=[nodes.LiteralString.make("qwe")])), env=env
    )
    assert not is_window_expression(nodes.FuncCall.make(name="avg", args=[nodes.LiteralString.make("qwe")]), env=env)
    assert not is_window_expression(nodes.FuncCall.make(name="func", args=[nodes.Field.make("my field")]), env=env)
    assert not is_window_expression(
        n.p(nodes.FuncCall.make(name="avg", args=[nodes.LiteralString.make("qwe")])), env=env
    )


def test_is_query_fork_expression():
    env = InspectionEnvironment()
    assert not is_query_fork_expression(
        nodes.WindowFuncCall.make(
            name="rank",
            args=[nodes.LiteralString.make("qwe")],
            grouping=nodes.WindowGroupingTotal(),
        ),
        env=env,
    )
    assert is_query_fork_expression(
        n.func.OTHERFUNC(
            n.fork(
                joining=n.joining(conditions=[n.self_condition(n.field("city"))]),
                result_expr=n.func.MY_FUNC(n.field("My Field")),
            ),
            n.field("Other Field"),
        ),
        env=env,
    )


def test_get_query_fork_nesting_level():
    env = InspectionEnvironment()
    assert (
        get_query_fork_nesting_level(
            n.formula(
                n.func.SOMEFUNC(
                    n.fork(
                        joining=n.joining(conditions=[n.self_condition(n.field("date"))]),
                        result_expr=n.func.OTHERFUNC(
                            n.fork(
                                joining=n.joining(conditions=[n.self_condition(n.field("city"))]),
                                result_expr=n.func.MY_FUNC(n.field("My Field")),
                            ),
                            n.field("Other Field"),
                        ),
                    )
                )
            ),
            env=env,
        )
        == 2
    )


def test_infer_data_type():
    env = InspectionEnvironment()
    field_types = {
        "My Field": DataType.STRING,
        "One Field": DataType.INTEGER,
        "Other Field": DataType.INTEGER,
    }
    assert (
        infer_data_type(
            nodes.Field.make("Other Field"),
            field_types=field_types,
            env=env,
        )
        == DataType.INTEGER
    )
    assert (
        infer_data_type(
            n.case(
                n.if_(n.func("==")(n.field("My Field"), n.p("qwe"))).then(True).else_(False),
            )
            .whens(
                n.when(True).then(3.4),
            )
            .else_(5.6),
            field_types=field_types,
            env=env,
        )
        == DataType.FLOAT
    )
    assert infer_data_type(n.wfunc.RANK(n.field("Other Field")), field_types=field_types, env=env) == DataType.INTEGER
    assert (
        infer_data_type(n.binary("==", n.field("One Field"), n.field("Other Field")), field_types=field_types, env=env)
        == DataType.BOOLEAN
    )


def test_enumerate():
    nhi = NodeHierarchyIndex.make
    main_func = nodes.FuncCall.make(
        name="concat",
        args=[
            nodes.Field.make("Other Field"),
            nodes.Field.make("Green Field"),
            nodes.LiteralString.make("qwerty"),
        ],
    )
    formula = n.formula(main_func)
    assert list(enumerate_fields(formula)) == [
        (nhi(0, 0), nodes.Field.make("Other Field")),
        (nhi(0, 1), nodes.Field.make("Green Field")),
    ]


def test_iter_operation_calls():
    names = [
        func_node.name
        for func_node in iter_operation_calls(
            n.func.CONCAT(
                n.func.INT(n.field("qwerty")),
                n.binary(">", n.field("f1"), n.field("f2")),
            )
        )
    ]
    assert names == ["concat", "int", ">"]
