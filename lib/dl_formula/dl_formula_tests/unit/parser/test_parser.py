from __future__ import annotations

import datetime

import pytest

from dl_formula.core import exc
from dl_formula.core.nodes import (
    BeforeFilterBy,
    Binary,
    CaseBlock,
    ExcludeLodSpecifier,
    ExpressionList,
    Field,
    FixedLodSpecifier,
    FuncCall,
    IfBlock,
    IfPart,
    IgnoreDimensions,
    IncludeLodSpecifier,
    LiteralBoolean,
    LiteralDate,
    LiteralFloat,
    LiteralGenericDatetime,
    LiteralInteger,
    LiteralString,
    Null,
    OrderAscending,
    OrderDescending,
    Ordering,
    Ternary,
    Unary,
    WhenPart,
    WindowFuncCall,
    WindowGroupingAmong,
    WindowGroupingTotal,
    WindowGroupingWithin,
)
from dl_formula.shortcuts import n


def test_keyword_case_insensitivity(parser):
    upper_formula = (
        'IF [n0]*5 THEN MY_FUNC(NULL, NOT [n2], "qwerty") ELSEIF ISNULL([n3]) THEN "result" '
        'ELSE CASE [n4] WHEN 5 THEN "five" WHEN 6 THEN "seven" ELSE "forty two" END END'
    )
    lower_formula = (
        'if [n0]*5 then MY_FUNC(null, not [n2], "qwerty") elseif ISNULL([n3]) then "result" '
        'else case [n4] when 5 then "five" when 6 then "seven" else "forty two" end end'
    )
    assert parser.parse(upper_formula) == parser.parse(lower_formula)


def test_position_getter(parser):
    formula = parser.parse(
        "IF [n0] * 5 " 'THEN MY_FUNC(NULL, NOT [n2], "qwerty") ' "ELSE [SOMETHING] AND [Q1] IN (12, 34) " "END"
    )
    assert formula.get_by_pos(1).__class__ is IfPart
    assert formula.get_by_pos(5).__class__ is Field
    assert formula.get_by_pos(8).__class__ is Binary
    assert formula.get_by_pos(20).__class__ is FuncCall
    assert formula.get_by_pos(30).__class__ is FuncCall
    assert formula.get_by_pos(33).__class__ is Unary
    assert formula.get_by_pos(47).__class__ is LiteralString
    assert formula.get_by_pos(84).__class__ is ExpressionList


def test_boolean_w_incomplete_grammar(parser):
    formula = parser.parse("true AND false")
    assert formula.get_by_pos(3).__class__ is LiteralBoolean
    assert formula.get_by_pos(11).__class__ is LiteralBoolean


def test_parse_multiline(parser):
    assert parser.parse("AVG(\n    1\n)") == n.formula(FuncCall.make("avg", [LiteralInteger.make(1)]))


def test_parse_boolean(parser):
    assert parser.parse("TRUE") == n.formula(LiteralBoolean.make(True))
    assert type(parser.parse("TRUE").expr.value) is bool
    assert parser.parse("FALSE") == n.formula(LiteralBoolean.make(False))
    assert type(parser.parse("FALSE").expr.value) is bool


def test_parse_integer(parser):
    assert parser.parse("123") == n.formula(LiteralInteger.make(123))
    assert type(parser.parse("123").expr.value) is int
    assert parser.parse("-123") == n.formula(LiteralInteger.make(-123))
    assert type(parser.parse("-123").expr.value) is int


def test_parse_float(parser):
    parse = parser.parse
    assert parse("123.0") == n.formula(LiteralFloat.make(123.0))
    assert type(parse("123.0").expr.value) is float
    assert parse("-123.0") == n.formula(LiteralFloat.make(-123.0))
    assert type(parse("-123.0").expr.value) is float
    assert parse("1e0") == n.formula(LiteralFloat.make(1.0))
    assert parse("1e2") == n.formula(LiteralFloat.make(100.0))
    assert parse("1e+2") == n.formula(LiteralFloat.make(100.0))
    assert parse("1e-2") == n.formula(LiteralFloat.make(0.01))
    assert parse("1.1e2") == n.formula(LiteralFloat.make(110.0))
    assert parse("1.1e+2") == n.formula(LiteralFloat.make(110.0))
    assert parse("1.1e-2") == n.formula(LiteralFloat.make(0.011))


def test_parse_null(parser):
    assert parser.parse("NULL") == n.formula(Null())


def test_parse_string(parser):
    assert parser.parse('"qwerty with spaces, \\", \\\' and -()"') == n.formula(
        LiteralString.make("qwerty with spaces, \", ' and -()")
    )
    assert parser.parse("'qwerty with spaces, \\\", \\', \\n, \\r, \\t and -()'") == n.formula(
        LiteralString.make("qwerty with spaces, \", ', \n, \r, \t and -()")
    )


def test_parse_field(parser):
    assert parser.parse("[my_field]") == n.formula(Field.make("my_field"))


def test_parse_parentheses(parser):
    assert parser.parse('("expr with parentheses")') == n.formula(n.p(LiteralString.make("expr with parentheses")))


def test_parse_date(parser):
    assert parser.parse("#2019-02-12#") == n.formula(LiteralDate.make(datetime.date(2019, 2, 12)))


def test_parse_datetime(parser):
    assert parser.parse("#2019-02-12T#") == n.formula(
        LiteralGenericDatetime.make(datetime.datetime(2019, 2, 12, 0, 0, 0))
    )
    assert parser.parse("#2019-02-12 12#") == n.formula(
        LiteralGenericDatetime.make(datetime.datetime(2019, 2, 12, 12, 0, 0))
    )
    assert parser.parse("#2019-02-12T12:34#") == n.formula(
        LiteralGenericDatetime.make(datetime.datetime(2019, 2, 12, 12, 34, 0))
    )
    assert parser.parse("#2019-02-12t12:34:56#") == n.formula(
        LiteralGenericDatetime.make(datetime.datetime(2019, 2, 12, 12, 34, 56))
    )


def test_parse_generic_datetime(parser):
    assert parser.parse("##2019-02-12##") == n.formula(LiteralDate.make(datetime.date(2019, 2, 12)))
    assert parser.parse("##2019-02-12T##") == n.formula(
        LiteralGenericDatetime.make(datetime.datetime(2019, 2, 12, 0, 0, 0))
    )
    assert parser.parse("##2019-02-12 12##") == n.formula(
        LiteralGenericDatetime.make(datetime.datetime(2019, 2, 12, 12, 0, 0))
    )
    assert parser.parse("##2019-02-12T12:34##") == n.formula(
        LiteralGenericDatetime.make(datetime.datetime(2019, 2, 12, 12, 34, 0))
    )
    assert parser.parse("##2019-02-12t12:34:56##") == n.formula(
        LiteralGenericDatetime.make(datetime.datetime(2019, 2, 12, 12, 34, 56))
    )


def test_parse_operations(parser):
    parse = parser.parse
    # ^
    assert parse("[left] ^ [right]") == n.formula(Binary.make("^", Field.make("left"), Field.make("right")))
    assert parse("[left]^-[right]") == n.formula(
        Binary.make("^", Field.make("left"), Unary.make("neg", Field.make("right")))
    )
    assert parse("[left] ^ NOT [right]") == n.formula(
        Binary.make("^", Field.make("left"), Unary.make("not", Field.make("right")))
    )
    assert parse("[left] IS NULL ^ [right]") == n.formula(
        Binary.make("^", Unary.make("isnull", Field.make("left")), Field.make("right"))
    )

    # - (negate)
    assert parse("-[expr]") == n.formula(Unary.make("neg", Field.make("expr")))
    assert parse(" -  - [expr]") == n.formula(Unary.make("neg", Unary.make("neg", Field.make("expr"))))
    assert parse("-[expr]^2") == n.formula(
        Unary.make("neg", Binary.make("^", Field.make("expr"), LiteralInteger.make(2)))
    )

    # *
    assert parse("[left] * [right]") == n.formula(Binary.make("*", Field.make("left"), Field.make("right")))
    assert parse("[left]*[right]") == n.formula(Binary.make("*", Field.make("left"), Field.make("right")))
    assert parse("123*123") == n.formula(Binary.make("*", LiteralInteger.make(123), LiteralInteger.make(123)))
    assert parse("[left]*-[right]") == n.formula(
        Binary.make("*", Field.make("left"), Unary.make("neg", Field.make("right")))
    )
    assert parse("[left] * NOT [right]") == n.formula(
        Binary.make("*", Field.make("left"), Unary.make("not", Field.make("right")))
    )
    assert parse("[left] IS NULL * [right]") == n.formula(
        Binary.make("*", Unary.make("isnull", Field.make("left")), Field.make("right"))
    )
    # /
    assert parse("[left] / [right]") == n.formula(Binary.make("/", Field.make("left"), Field.make("right")))
    assert parse("[left] / -[right]") == n.formula(
        Binary.make("/", Field.make("left"), Unary.make("neg", Field.make("right")))
    )
    assert parse("[left] / NOT [right]") == n.formula(
        Binary.make("/", Field.make("left"), Unary.make("not", Field.make("right")))
    )
    assert parse("[left] IS NULL / [right]") == n.formula(
        Binary.make("/", Unary.make("isnull", Field.make("left")), Field.make("right"))
    )
    # %
    assert parse("[left] % [right]") == n.formula(Binary.make("%", Field.make("left"), Field.make("right")))
    assert parse("[left] % -[right]") == n.formula(
        Binary.make("%", Field.make("left"), Unary.make("neg", Field.make("right")))
    )
    assert parse("[left] % NOT [right]") == n.formula(
        Binary.make("%", Field.make("left"), Unary.make("not", Field.make("right")))
    )
    assert parse("[left] IS NULL % [right]") == n.formula(
        Binary.make("%", Unary.make("isnull", Field.make("left")), Field.make("right"))
    )
    # chained
    assert parse("[n1] / [n2] * [n3] % [n1]") == n.formula(
        Binary.make(
            "%",
            Binary.make(
                "*",
                Binary.make("/", Field.make("n1"), Field.make("n2")),
                Field.make("n3"),
            ),
            Field.make("n1"),
        )
    )
    # priority: negation vs * and /
    assert parse("-[a] * [b]") == n.formula(
        Binary.make(
            "*",
            Unary.make("neg", Field.make("a")),
            Field.make("b"),
        )
    )
    assert parse("[c] / -[a] * [b]") == n.formula(
        Binary.make(
            "*",
            Binary.make("/", Field.make("c"), Unary.make("neg", Field.make("a"))),
            Field.make("b"),
        )
    )
    # priority: * vs ^
    assert parse("[n1] * [n2] ^ [n3]") == n.formula(
        Binary.make(
            "*",
            Field.make("n1"),
            Binary.make("^", Field.make("n2"), Field.make("n3")),
        )
    )

    # +
    assert parse("[left] + [right]") == n.formula(Binary.make("+", Field.make("left"), Field.make("right")))
    assert parse("[left] + NOT [right]") == n.formula(
        Binary.make("+", Field.make("left"), Unary.make("not", Field.make("right")))
    )
    assert parse("[left] IS NULL + [right]") == n.formula(
        Binary.make("+", Unary.make("isnull", Field.make("left")), Field.make("right"))
    )
    # -
    assert parse("[left] - [right]") == n.formula(Binary.make("-", Field.make("left"), Field.make("right")))
    assert parse("[left] - NOT [right]") == n.formula(
        Binary.make("-", Field.make("left"), Unary.make("not", Field.make("right")))
    )
    assert parse("[left] IS NULL - [right]") == n.formula(
        Binary.make("-", Unary.make("isnull", Field.make("left")), Field.make("right"))
    )
    # chained +/-
    assert parse("[n1] - [n2] + [n3]") == n.formula(
        Binary.make(
            "+",
            Binary.make("-", Field.make("n1"), Field.make("n2")),
            Field.make("n3"),
        )
    )
    # priority: + vs *
    assert parse("[n1] + [n2] * [n3]") == n.formula(
        Binary.make(
            "+",
            Field.make("n1"),
            Binary.make("*", Field.make("n2"), Field.make("n3")),
        )
    )

    # is ...
    assert parse("[left] is null") == n.formula(Unary.make("isnull", Field.make("left")))
    assert parse("[left] is not null") == n.formula(Unary.make("not", Unary.make("isnull", Field.make("left"))))
    assert parse("[left] is true") == n.formula(Unary.make("istrue", Field.make("left")))
    assert parse("[left] is not true") == n.formula(Unary.make("not", Unary.make("istrue", Field.make("left"))))
    assert parse("[left] is false") == n.formula(Unary.make("isfalse", Field.make("left")))
    assert parse("[left] is not false") == n.formula(Unary.make("not", Unary.make("isfalse", Field.make("left"))))
    # priority: is vs +
    assert parse("[n1] + [n2] is null") == n.formula(
        Unary.make("isnull", Binary.make("+", Field.make("n1"), Field.make("n2")))
    )

    # like
    assert parse("[left] like [right]") == n.formula(Binary.make("like", Field.make("left"), Field.make("right")))
    assert parse("[left] not like [right]") == n.formula(
        Binary.make("notlike", Field.make("left"), Field.make("right"))
    )
    # priority: like vs is
    assert parse("[n1] like [n2] is null") == n.formula(
        Binary.make("like", Field.make("n1"), Unary.make("isnull", Field.make("n2")))
    )

    # =
    assert parse("[left] = [right]") == n.formula(Binary.make("==", Field.make("left"), Field.make("right")))
    assert parse("[left]=[right]") == n.formula(Binary.make("==", Field.make("left"), Field.make("right")))
    assert parse("123=123") == n.formula(Binary.make("==", LiteralInteger.make(123), LiteralInteger.make(123)))
    assert parse("[left] = NOT [right]") == n.formula(
        Binary.make("==", Field.make("left"), Unary.make("not", Field.make("right")))
    )
    # !=
    assert parse("[left] != [right]") == n.formula(Binary.make("!=", Field.make("left"), Field.make("right")))
    assert parse("[left] <> [right]") == n.formula(Binary.make("!=", Field.make("left"), Field.make("right")))
    # >
    assert parse("[left] > [right]") == n.formula(Binary.make(">", Field.make("left"), Field.make("right")))
    # >=
    assert parse("[left] >= [right]") == n.formula(Binary.make(">=", Field.make("left"), Field.make("right")))
    # <
    assert parse("[left] < [right]") == n.formula(Binary.make("<", Field.make("left"), Field.make("right")))
    # <=
    assert parse("[left] <= [right]") == n.formula(Binary.make("<=", Field.make("left"), Field.make("right")))
    # chained eq/neq
    assert parse("[n1] < [n2] = [n3] > [n4]") == n.formula(
        Binary.make(
            "and",
            Binary.make(
                "and",
                Binary.make("<", Field.make("n1"), Field.make("n2")),
                Binary.make("==", Field.make("n2"), Field.make("n3")),
            ),
            Binary.make(">", Field.make("n3"), Field.make("n4")),
        )
    )
    # priority: <> vs or
    assert parse("[n1] < [n2] or [n3] > [n4]") == n.formula(
        Binary.make(
            "or",
            Binary.make("<", Field.make("n1"), Field.make("n2")),
            Binary.make(">", Field.make("n3"), Field.make("n4")),
        ),
    )
    # priority: = vs like
    assert parse("[n1] = [n2] like [n3]") == n.formula(
        Binary.make(
            "==",
            Field.make("n1"),
            Binary.make("like", Field.make("n2"), Field.make("n3")),
        )
    )

    # BETWEEN / NOT BETWEEN
    assert parse("[my_field] BETWEEN 123 AND 456") == n.formula(
        Ternary.make("between", Field.make("my_field"), LiteralInteger.make(123), LiteralInteger.make(456))
    )
    assert parse("[my_field]NOT BETWEEN(123)AND(456)") == n.formula(
        Ternary.make("notbetween", Field.make("my_field"), n.p(LiteralInteger.make(123)), n.p(LiteralInteger.make(456)))
    )
    # priority: BETWEEN vs comparison
    assert parse("45 BETWEEN [n2] = 123 AND 456") == n.formula(
        Ternary.make(
            "between",
            LiteralInteger.make(45),
            Binary.make("==", Field.make("n2"), LiteralInteger.make(123)),
            LiteralInteger.make(456),
        )
    )
    assert parse("[first] BETWEEN NOT [second] AND NOT [third]") == n.formula(
        Ternary.make(
            "between",
            Field.make("first"),
            Unary.make("not", Field.make("second")),
            Unary.make("not", Field.make("third")),
        )
    )
    # priority: BETWEEN vs AND
    assert parse("[first] BETWEEN [second] AND [third] AND [fourth]") == n.formula(
        Binary.make(
            "and",
            Ternary.make("between", Field.make("first"), Field.make("second"), Field.make("third")),
            Field.make("fourth"),
        ),
    )

    # IN / NOT IN
    assert parse("[my_field] IN (123, 456)") == n.formula(
        Binary.make(
            "in", Field.make("my_field"), ExpressionList.make(LiteralInteger.make(123), LiteralInteger.make(456))
        )
    )
    assert parse("[my_field]NOT IN(123, 456)") == n.formula(
        Binary.make(
            "notin", Field.make("my_field"), ExpressionList.make(LiteralInteger.make(123), LiteralInteger.make(456))
        )
    )
    # priority: IN vs BETWEEN
    assert parse("[n2] BETWEEN 1 AND 2 IN (2)") == n.formula(
        Binary.make(
            "in",
            Ternary.make("between", Field.make("n2"), LiteralInteger.make(1), LiteralInteger.make(2)),
            ExpressionList.make(LiteralInteger.make(2)),
        )
    )

    # NOT
    assert parse("NOT [my_field]") == n.formula(Unary.make("not", Field.make("my_field")))
    assert parse("NOT[my_field]") == n.formula(Unary.make("not", Field.make("my_field")))
    assert parse("NOT 123") == n.formula(Unary.make("not", LiteralInteger.make(123)))
    assert parse("NOT NOT [my_field]") == n.formula(Unary.make("not", Unary.make("not", Field.make("my_field"))))
    assert parse("NOT(123)") == n.formula(Unary.make("not", n.p(LiteralInteger.make(123))))
    # priority: NOT vs IN
    assert parse("NOT [n1] IN (1)") == n.formula(
        Unary.make("not", Binary.make("in", Field.make("n1"), ExpressionList.make(LiteralInteger.make(1))))
    )

    # AND
    assert parse("[left] AND [right]") == n.formula(Binary.make("and", Field.make("left"), Field.make("right")))
    assert parse("[left]AND[right]") == n.formula(Binary.make("and", Field.make("left"), Field.make("right")))
    assert parse("123 AND 123") == n.formula(Binary.make("and", LiteralInteger.make(123), LiteralInteger.make(123)))
    assert parse("[n1] AND [n2] AND [n3]") == n.formula(
        Binary.make(
            "and",
            Binary.make("and", Field.make("n1"), Field.make("n2")),
            Field.make("n3"),
        )
    )
    # priority: AND vs NOT
    assert parse("NOT [n1] AND [n2]") == n.formula(
        Binary.make("and", Unary.make("not", Field.make("n1")), Field.make("n2"))
    )

    # OR
    assert parse("[left] OR [right]") == n.formula(Binary.make("or", Field.make("left"), Field.make("right")))
    assert parse("[left]OR[right]") == n.formula(Binary.make("or", Field.make("left"), Field.make("right")))
    assert parse("123 OR 123") == n.formula(Binary.make("or", LiteralInteger.make(123), LiteralInteger.make(123)))
    assert parse("[n1] OR [n2] OR [n3]") == n.formula(
        Binary.make(
            "or",
            Binary.make("or", Field.make("n1"), Field.make("n2")),
            Field.make("n3"),
        )
    )

    # priority: OR vs AND
    assert parse("[n1] OR [n2] AND [n3]") == n.formula(
        Binary.make(
            "or",
            Field.make("n1"),
            Binary.make("and", Field.make("n2"), Field.make("n3")),
        )
    )


def test_parse_if_block(parser):
    assert parser.parse('IF "cond_1" THEN "res_1" ELSEIF "cond_2" THEN "res_2" ELSE "res_3" END') == n.formula(
        IfBlock.make(
            if_list=[
                IfPart.make(LiteralString.make("cond_1"), LiteralString.make("res_1")),
                IfPart.make(LiteralString.make("cond_2"), LiteralString.make("res_2")),
            ],
            else_expr=LiteralString.make("res_3"),
        )
    )

    # nested IF blocks
    # TODO: flatten ELSE-nested IF blocks?
    assert parser.parse('IF "cond_1" THEN "res_1" ELSE IF "cond_2" THEN "res_2" ELSE "res_3" END END') == n.formula(
        IfBlock.make(
            if_list=[
                IfPart.make(LiteralString.make("cond_1"), LiteralString.make("res_1")),
            ],
            else_expr=IfBlock.make(
                if_list=[IfPart.make(LiteralString.make("cond_2"), LiteralString.make("res_2"))],
                else_expr=LiteralString.make("res_3"),
            ),
        )
    )


def test_parse_case_block(parser):
    assert parser.parse(
        'CASE [expr_0] WHEN "val_1" THEN "res_1" WHEN "val_2" THEN "res_2" ELSE "res_3" END'
    ) == n.formula(
        CaseBlock.make(
            case_expr=Field.make("expr_0"),
            when_list=[
                WhenPart.make(LiteralString.make("val_1"), LiteralString.make("res_1")),
                WhenPart.make(LiteralString.make("val_2"), LiteralString.make("res_2")),
            ],
            else_expr=LiteralString.make("res_3"),
        )
    )


def test_parse_func_call(parser):
    assert parser.parse("my_func()") == n.formula(FuncCall.make(name="my_func", args=[]))
    assert parser.parse('my_func("s")') == n.formula(FuncCall.make(name="my_func", args=[LiteralString.make("s")]))
    assert parser.parse('my_func("s", 45.6, [my_field])') == n.formula(
        FuncCall.make(name="my_func", args=[LiteralString.make("s"), LiteralFloat.make(45.6), Field.make("my_field")])
    )


def test_parse_win_func_call(parser):
    assert parser.parse("mavg()") == n.formula(
        WindowFuncCall.make(name="mavg", args=[], grouping=WindowGroupingTotal())
    )
    assert parser.parse("mavg(TOTAL)") == n.formula(
        WindowFuncCall.make(name="mavg", args=[], grouping=WindowGroupingTotal())
    )
    assert parser.parse("mavg([my_field] AMONG [dim1], [dim2])") == n.formula(
        WindowFuncCall.make(
            name="mavg",
            args=[Field.make("my_field")],
            grouping=WindowGroupingAmong.make(dim_list=[Field.make("dim1"), Field.make("dim2")]),
        )
    )
    assert parser.parse("mavg([my_field] WITHIN [dim1], [dim2])") == n.formula(
        WindowFuncCall.make(
            name="mavg",
            args=[Field.make("my_field")],
            grouping=WindowGroupingWithin.make(
                dim_list=[
                    Field.make("dim1"),
                    Field.make("dim2"),
                ]
            ),
        )
    )


def test_agg_with_lod(parser):
    assert parser.parse("sum([my_field] FIXED [dim1], [dim2])") == n.formula(
        FuncCall.make(
            name="sum",
            args=[Field.make("my_field")],
            lod=FixedLodSpecifier.make(dim_list=[Field.make("dim1"), Field.make("dim2")]),
        )
    )
    assert parser.parse("sum([my_field] INCLUDE [dim1], [dim2])") == n.formula(
        FuncCall.make(
            name="sum",
            args=[Field.make("my_field")],
            lod=IncludeLodSpecifier.make(
                dim_list=[
                    Field.make("dim1"),
                    Field.make("dim2"),
                ]
            ),
        )
    )
    assert parser.parse("sum([my_field] EXCLUDE [dim1], [dim2])") == n.formula(
        FuncCall.make(
            name="sum",
            args=[Field.make("my_field")],
            lod=ExcludeLodSpecifier.make(
                dim_list=[
                    Field.make("dim1"),
                    Field.make("dim2"),
                ]
            ),
        )
    )


def test_parse_win_func_call_with_order_by(parser):
    assert parser.parse("mavg([my_field] WITHIN [dim1], [dim2] ORDER BY [dim3])") == n.formula(
        WindowFuncCall.make(
            name="mavg",
            args=[Field.make("my_field")],
            grouping=WindowGroupingWithin.make(
                dim_list=[
                    Field.make("dim1"),
                    Field.make("dim2"),
                ]
            ),
            ordering=Ordering.make(
                expr_list=[
                    Field.make("dim3"),
                ]
            ),
        )
    )
    assert parser.parse("mavg([my_field] ORDER BY [dim3])") == n.formula(
        WindowFuncCall.make(
            name="mavg",
            args=[Field.make("my_field")],
            ordering=Ordering.make(
                expr_list=[
                    Field.make("dim3"),
                ]
            ),
        )
    )
    assert parser.parse("mavg([my_field] ORDER BY [dim1], [dim2] DESC, [dim3] ASC)") == n.formula(
        WindowFuncCall.make(
            name="mavg",
            args=[Field.make("my_field")],
            ordering=Ordering.make(
                expr_list=[
                    Field.make("dim1"),
                    OrderDescending.make(expr=Field.make("dim2")),
                    OrderAscending.make(expr=Field.make("dim3")),
                ]
            ),
        )
    )


def test_parse_win_func_call_with_before_filter_by(parser):
    assert parser.parse(
        "mavg([my_field] WITHIN [dim1], [dim2] ORDER BY [dim3] BEFORE FILTER BY [dim4], [dim5])"
    ) == n.formula(
        WindowFuncCall.make(
            name="mavg",
            args=[Field.make("my_field")],
            grouping=WindowGroupingWithin.make(
                dim_list=[
                    Field.make("dim1"),
                    Field.make("dim2"),
                ]
            ),
            ordering=Ordering.make(
                expr_list=[
                    Field.make("dim3"),
                ]
            ),
            before_filter_by=BeforeFilterBy.make(field_names=frozenset(["dim4", "dim5"])),
        )
    )
    assert parser.parse("mavg([my_field] BEFORE FILTER BY [dim3])") == n.formula(
        WindowFuncCall.make(
            name="mavg",
            args=[Field.make("my_field")],
            before_filter_by=BeforeFilterBy.make(field_names=frozenset(["dim3"])),
        )
    )


def test_parse_func_call_with_ignore_dimensions(parser):
    assert parser.parse("ago([my_field] IGNORE DIMENSIONS [dim1], [dim2])") == n.formula(
        FuncCall.make(
            name="ago",
            args=[Field.make("my_field")],
            ignore_dimensions=IgnoreDimensions.make(dim_list=(Field.make("dim1"), Field.make("dim2"))),
        )
    )


def test_comments_basic(parser):
    parse = parser.parse
    # Comments
    assert parse("[abc] -- [zxc]") == parse("[abc]")
    assert parse("[abc]-- [zxc]") == parse("[abc]")
    assert parse("[abc]--[zxc]") == parse("[abc]")
    assert parse("[abc] /* [zxc] */") == parse("[abc]")
    assert parse("[abc] /* [zxc]/* */") == parse("[abc]")
    assert parse('[abc] /* zxc" /**/') == parse("[abc]")
    assert parse("[abc] /* /* -- */") == parse("[abc]")
    assert parse(' -- " "\n"abc"') == parse('"abc"')

    # Not comments
    assert parse('" /* " /* */') == n.formula(LiteralString.make(value=" /* "))
    assert parse('" -- "') == n.formula(LiteralString.make(value=" -- "))
    assert parse("[ab -- c]") == n.formula(Field.make(name="ab -- c"))
    assert parse("[ab/*c*/]") == n.formula(Field.make(name="ab/*c*/"))
    double_minus = n.formula(
        expr=Binary.make(
            name="-",
            left=Field.make(name="abc"),
            right=Unary.make(name="neg", expr=Field.make(name="zxc")),
        )
    )
    assert parse("[abc] - - [zxc]") == double_minus
    assert parse("[abc]- - [zxc]") == double_minus
    assert parse("[abc] - -[zxc]") == double_minus
    assert parse("[abc]- -[zxc]") == double_minus

    # Invalid
    with pytest.raises(exc.ParseError):
        parse('" /* " */"')
    with pytest.raises(exc.ParseError):
        parse('" -- " \\n"')
    with pytest.raises(exc.ParseError):
        parse('[abc] /* "zxc */"*/')


def test_comments_in_if(parser):
    expected = parser.parse('IF "hi" THEN "world" END')

    def check(formula: str) -> None:
        assert parser.parse(formula) == expected

    # Single-line comments
    check('IF -- comment\n"hi" THEN "world" END')
    check('IF "hi" THEN "world" END-- comment\n')
    check('IF "hi" THEN "world" END-- comment')
    check('IF "hi" -- first\n THEN-- second \n"world" END -- third')

    # Multi-line comments
    check('IF /* comment */ "hi" THEN "world" END')
    check('IF /* co\nmm\nent */ "hi" THEN "world" END')

    # Mixed
    check('IF "hi" --first\n THEN /*  second*/ "world" END -- third')


def test_unexpected_char(parser):
    with pytest.raises(exc.ParseError) as exc_info:
        parser.parse("[n1]\n+ )")

    err = exc_info.value
    assert err.error.coords == (1, 2)
    assert err.error.token == ")"
    assert err.error.code == ("FORMULA", "PARSE", "UNEXPECTED_TOKEN")


def test_unexpected_end(parser):
    with pytest.raises(exc.ParseError) as exc_info:
        parser.parse("[n1]\n+ ")

    err = exc_info.value
    assert err.error.coords == (1, 2)
    assert err.error.token is None
    assert err.error.code == ("FORMULA", "PARSE", "UNEXPECTED_EOF")


def test_invalid_date_value(parser):
    with pytest.raises(exc.ParseError) as exc_info:
        parser.parse("#2010-13-45#")

    err = exc_info.value
    assert err.error.coords == (0, 0)
    assert err.error.token == "2010-13-45"
    assert err.error.code == ("FORMULA", "PARSE", "VALUE", "DATE")


def test_invalid_datetime_value(parser):
    with pytest.raises(exc.ParseError) as exc_info:
        parser.parse("#2010-04-02 28:00:00#")

    err = exc_info.value
    assert err.error.coords == (0, 0)
    assert err.error.token == "2010-04-02 28:00:00"
    assert err.error.code == ("FORMULA", "PARSE", "VALUE", "DATETIME")


def test_inconceivably_long_if_block(parser):
    # Note: at around 500 it timeouts the whole unit test too easily. Might
    # need to move it to `medium` tests to test this much.
    num = 300
    formula = 'if contains("s", [v]) then "a"\n'
    if_list = [
        IfPart.make(
            cond=FuncCall.make("contains", [LiteralString.make("s"), Field.make("v")]), expr=LiteralString.make("a")
        )
    ]
    for if_num in range(num):
        formula += f'elseif contains("s_{if_num}", [v]) then "a_{if_num}"\n'
        if_list.append(
            IfPart.make(
                cond=FuncCall.make("contains", [LiteralString.make(f"s_{if_num}"), Field.make("v")]),
                expr=LiteralString.make(f"a_{if_num}"),
            )
        )
    formula += 'else "" end'
    expected_result = n.formula(IfBlock.make(if_list=if_list, else_expr=LiteralString.make("")))
    actual_result = parser.parse(formula)
    assert actual_result == expected_result


def test_recursion_error(parser):
    # Create large code that will cause RecursionError
    num = 1000

    code = ""
    for if_num in range(num):
        code += f'if([v] = {if_num}, "v_{if_num}", '
    code += '"v_max"'
    code += ")" * num

    with pytest.raises(exc.ParseError) as exc_info:
        parser.parse(code)

    err = exc_info.value
    assert err.error.code == ("FORMULA", "PARSE", "RECURSION")
