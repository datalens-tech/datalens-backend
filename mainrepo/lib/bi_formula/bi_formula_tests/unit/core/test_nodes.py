from __future__ import annotations

import copy
from typing import Dict

from bi_formula.core import nodes
from bi_formula.core.index import NodeHierarchyIndex
from bi_formula.core.position import Position


def test_equality():
    assert nodes.LiteralString.make("a") == nodes.LiteralString.make("a")
    assert nodes.Field.make(name="a") == nodes.Field.make(name="a")
    assert nodes.FuncCall.make(name="a", args=[nodes.LiteralInteger.make(123)]) == (
        nodes.FuncCall.make(name="a", args=[nodes.LiteralInteger.make(123)])
    )


def test_copy():
    def _check_node_copy(node: nodes.FormulaItem) -> None:
        node_copy = copy.copy(node)
        assert node_copy == node
        assert node_copy is not node
        assert len(node.children) == len(node.children)
        for i in range(len(node.children)):
            assert node_copy.children[i] == node.children[i]
            assert node_copy.children[i] is not node.children[i]
        assert node_copy.original_text == node.original_text
        assert node_copy.position == node.position

    _check_node_copy(nodes.LiteralInteger.make(123, meta=nodes.NodeMeta(position=Position(1, 2, 0, 0, 1, 2))))
    _check_node_copy(nodes.Field.make("qwerty"))
    _check_node_copy(nodes.FuncCall.make("func", args=[nodes.LiteralInteger.make(123)]))
    _check_node_copy(
        nodes.FuncCall.make("func", args=[nodes.LiteralInteger.make(123), nodes.LiteralString.make("qwerty")])
    )
    _check_node_copy(nodes.Unary.make("un", nodes.LiteralInteger.make(123)))
    _check_node_copy(nodes.Binary.make("bin", nodes.LiteralInteger.make(123), nodes.LiteralString.make("qwerty")))
    _check_node_copy(
        nodes.Ternary.make(
            "tern", nodes.LiteralInteger.make(123), nodes.LiteralString.make("qwerty"), nodes.LiteralInteger.make(34)
        )
    )
    _check_node_copy(
        nodes.WindowFuncCall.make(
            "wfunc",
            args=[nodes.LiteralInteger.make(123)],
            grouping=nodes.WindowGroupingWithin.make(dim_list=[nodes.Field.make(name="my_field")]),
        )
    )
    _check_node_copy(
        nodes.WindowFuncCall.make(
            "owfunc",
            args=[nodes.LiteralInteger.make(123)],
            grouping=nodes.WindowGroupingWithin.make(
                dim_list=[
                    nodes.Field.make(name="my_field"),
                ]
            ),
            ordering=nodes.Ordering.make(
                expr_list=[
                    nodes.Field.make("dim"),
                ]
            ),
            before_filter_by=nodes.BeforeFilterBy.make(
                field_names=[
                    "field_1",
                    "field_3",
                    "field_2",
                ]
            ),
        )
    )
    _check_node_copy(
        nodes.IfBlock.make(
            [
                nodes.IfPart.make(nodes.LiteralBoolean.make(True), nodes.LiteralString.make("qwerty")),
                nodes.IfPart.make(nodes.LiteralBoolean.make(True), nodes.LiteralString.make("qwerty")),
            ],
            nodes.LiteralString.make("abc"),
        )
    )
    _check_node_copy(
        nodes.CaseBlock.make(
            nodes.LiteralString.make("val"),
            [
                nodes.WhenPart.make(nodes.LiteralString.make("case1"), nodes.LiteralString.make("qwerty")),
                nodes.WhenPart.make(nodes.LiteralString.make("case2"), nodes.LiteralString.make("asdfg")),
            ],
            nodes.LiteralString.make("abc"),
        )
    )
    _check_node_copy(
        nodes.Formula.make(
            expr=nodes.LiteralInteger.make(123),
            meta=nodes.NodeMeta(position=Position(0, 3, 0, 0, 0, 3), original_text="123"),
        )
    )


def test_light_copy():
    def _check_node_copy(
        src: nodes.FormulaItem,
        to_replace_map: Dict[int, nodes.FormulaItem] = None,
        expected: nodes.FormulaItem = None,
    ):
        assert src.light_copy(src.children) == src

        if to_replace_map is None:
            return

        children = [to_replace_map[idx] if idx in to_replace_map else child for idx, child in enumerate(src.children)]
        actual = src.light_copy(children)
        assert actual == expected

        assert len(actual.children) == len(expected.children)
        for idx, expected_child in enumerate(expected.children):
            assert actual.children[idx] == expected_child
            if idx not in to_replace_map:
                assert actual.children[idx] is src.children[idx]

        assert actual.original_text == expected.original_text
        assert actual.position == expected.position

    _check_node_copy(nodes.LiteralInteger.make(123, meta=nodes.NodeMeta(position=Position(1, 2, 0, 0, 1, 2))))
    _check_node_copy(nodes.Field.make("qwerty"))

    lit_123 = nodes.LiteralInteger.make(123)
    lit_234 = nodes.LiteralInteger.make(234)
    lit_x = nodes.LiteralString.make("x")

    field_my = nodes.Field.make(name="my_field")
    win_grouping_within_my_field = nodes.WindowGroupingWithin.make(dim_list=[field_my])
    win_grouping_within_my_field_2 = nodes.WindowGroupingWithin.make(dim_list=[nodes.Field.make(name="my_field2")])

    if_lit_123 = nodes.IfPart.make(nodes.LiteralBoolean.make(True), lit_123)
    if_lit_x = nodes.IfPart.make(nodes.LiteralBoolean.make(True), lit_x)

    case_lit_1_123 = nodes.WhenPart.make(nodes.LiteralString.make("case1"), lit_123)
    case_lit_1_x = nodes.WhenPart.make(nodes.LiteralString.make("case1"), lit_x)
    case_lit_2_234 = nodes.WhenPart.make(nodes.LiteralString.make("case2"), lit_234)

    _check_node_copy(
        nodes.FuncCall.make("func", args=[lit_123, lit_x]),
        {0: lit_234},
        nodes.FuncCall.make("func", args=[lit_234, lit_x]),
    )
    _check_node_copy(
        nodes.Unary.make("un", lit_123),
        {0: lit_234},
        nodes.Unary.make("un", lit_234),
    )
    _check_node_copy(
        nodes.Binary.make("bin", lit_123, nodes.LiteralString.make("qwerty")),
        {1: lit_234},
        nodes.Binary.make("bin", lit_123, lit_234),
    )
    _check_node_copy(
        nodes.Ternary.make("tern", lit_123, nodes.LiteralString.make("qwerty"), lit_234),
        {1: lit_x, 2: lit_123},
        nodes.Ternary.make("tern", lit_123, lit_x, lit_123),
    )
    _check_node_copy(
        nodes.WindowFuncCall.make("wfunc", args=[lit_123], grouping=win_grouping_within_my_field),
        {2: win_grouping_within_my_field_2},  # 0 single arg, 1  ordering, 2 grouping ...
        nodes.WindowFuncCall.make(
            "wfunc",
            args=[lit_123],
            grouping=win_grouping_within_my_field_2,
        ),
    )
    _check_node_copy(
        nodes.IfBlock.make([if_lit_123, if_lit_x], lit_234),
        {1: if_lit_123},
        nodes.IfBlock.make([if_lit_123, if_lit_123], lit_234),
    )
    _check_node_copy(
        nodes.CaseBlock.make(field_my, [case_lit_1_123, case_lit_2_234], lit_x),
        {1: case_lit_1_x, 3: lit_123},
        nodes.CaseBlock.make(field_my, [case_lit_1_x, case_lit_2_234], lit_123),
    )
    meta_1 = nodes.NodeMeta(position=Position(0, 3, 0, 0, 0, 3), original_text="123")
    _check_node_copy(
        nodes.Formula.make(
            expr=lit_123,
            meta=meta_1,
        ),
        {0: lit_x},
        nodes.Formula.make(
            expr=lit_x,
            meta=meta_1,
        ),
    )


def test_search_with_none_pos_range():
    f = nodes.Formula.make(
        expr=nodes.LiteralString.make("op"),
        meta=nodes.NodeMeta(position=Position(0, 2, 0, 0, 0, 2), original_text="op"),
    )
    assert f.get_by_pos(1) is f


def test_get():
    the_node = nodes.Field.make("My Field")
    index = NodeHierarchyIndex.make(0, 1)
    assert (
        nodes.Formula.make(
            nodes.FuncCall.make(
                name="concat",
                args=[
                    nodes.Field.make("Other Field"),
                    the_node,
                    nodes.LiteralString.make("qwerty"),
                ],
            )
        )[index]
        is the_node
    )


def test_replace_at_index():
    index = NodeHierarchyIndex.make(0, 1)
    updated_formula = nodes.Formula.make(
        nodes.FuncCall.make(
            name="concat",
            args=[
                nodes.Field.make("Other Field"),
                nodes.Field.make("Old Field"),
                nodes.LiteralString.make("qwerty"),
            ],
        )
    ).replace_at_index(index, nodes.Field.make("New Field"))
    expected_formula = nodes.Formula.make(
        nodes.FuncCall.make(
            name="concat",
            args=[
                nodes.Field.make("Other Field"),
                nodes.Field.make("New Field"),
                nodes.LiteralString.make("qwerty"),
            ],
        )
    )
    assert updated_formula == expected_formula

    # Also make sure that extracts match
    for index, updated_child in updated_formula.enumerate():
        expected_child = expected_formula[index]
        assert updated_child.extract == expected_child.extract


def test_substitute_batch():
    nhi = NodeHierarchyIndex.make
    r1 = nodes.Field.make("R1")
    r2 = nodes.Field.make("R2")

    updated_formula = nodes.Formula.make(
        nodes.FuncCall.make(
            name="concat",
            args=[
                nodes.Field.make("F1"),
                nodes.Field.make("F1"),
                nodes.Field.make("F2"),
                nodes.Field.make("F3"),
            ],
        )
    ).substitute_batch(
        {
            nhi(0, 0): nodes.Field.make("R1"),
            nhi(0, 1): nodes.Field.make("R1"),
            nhi(0, 2): nodes.Field.make("R2"),
        }
    )
    expected_formula = nodes.Formula.make(
        nodes.FuncCall.make(
            name="concat",
            args=[
                r1,
                r1,
                r2,
                nodes.Field.make("F3"),
            ],
        )
    )
    assert updated_formula == expected_formula

    # Also make sure that extracts match
    for index, updated_child in updated_formula.enumerate():
        expected_child = expected_formula[index]
        assert updated_child.extract == expected_child.extract


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
    formula = nodes.Formula.make(main_func)
    assert list(formula.enumerate()) == [
        (nhi(), formula),
        (nhi(0), main_func),
        (nhi(0, 0), nodes.Field.make("Other Field")),
        (nhi(0, 1), nodes.Field.make("Green Field")),
        (nhi(0, 2), nodes.LiteralString.make("qwerty")),
        (nhi(0, 3), nodes.DefaultAggregationLodSpecifier()),  # implicitly created
        (nhi(0, 4), nodes.IgnoreDimensions()),  # implicitly created
        (nhi(0, 5), nodes.BeforeFilterBy.make()),  # implicitly created
    ]


def test_iter_index():
    main_func = nodes.FuncCall.make(
        name="concat",
        args=[
            nodes.Field.make("Other Field"),
            nodes.Field.make("Green Field"),
            nodes.LiteralString.make("qwerty"),
        ],
    )
    formula = nodes.Formula.make(main_func)
    assert list(formula.iter_index(index=NodeHierarchyIndex.make(0, 1))) == [
        formula,
        main_func,
        nodes.Field.make("Green Field"),
    ]
    assert list(formula.iter_index(index=NodeHierarchyIndex.make(0, 1), exclude_last=True)) == [
        formula,
        main_func,
    ]
