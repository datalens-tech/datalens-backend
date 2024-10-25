from __future__ import annotations

import pytest

from dl_formula.scripts.formula_cli import (
    FormulaCliTool,
    parser,
)
from dl_formula_testing.tool_runner import ToolRunner


@pytest.fixture(scope="module")
def tool():
    return ToolRunner(parser=parser, tool_cls=FormulaCliTool)


def test_parse(tool):
    stdout, stderr = tool.run(["parse", "my_func(123)"])
    assert stdout == "Formula(expr=FuncCall(name='my_func', args=(LiteralInteger(value=123),)))\n"
    assert stderr == ""

    stdout, stderr = tool.run(["parse", "--pretty", "--with-meta", "very_very_long_func_name(nested_func())"])
    assert (
        stdout
        == """Formula(
  expr=FuncCall(
    name='very_very_long_func_name',
    args=(
      FuncCall(
        name='nested_func',
        args=(),
        meta=NodeMeta(position=Position(start=25, end=38, start_row=0, end_row=0, start_col=25, end_col=38)),
      ),
    ),
    meta=NodeMeta(position=Position(start=0, end=39, start_row=0, end_row=0, start_col=0, end_col=39)),
  ),
  meta=NodeMeta(position=Position(start=0, end=39, start_row=0, end_row=0, start_col=0, end_col=39)),
)
"""
    )
    assert stderr == ""

    stdout, stderr = tool.run(
        [
            "parse",
            "--suppress-errors",
        ],
        stdin=("SUM(1)\n" "AVG(2)\n" "INVALID_FORMULA([n1"),
    )
    assert stdout.strip() == (
        "Formula(expr=FuncCall(name='sum', args=(LiteralInteger(value=1),)))\n"
        "Formula(expr=FuncCall(name='avg', args=(LiteralInteger(value=2),)))\n"
        "#ERROR"
    )
    assert stderr == ""


def test_translate(tool):
    stdout, stderr = tool.run(
        [
            "translate",
            "--table",
            "my_table",
            "--dialect",
            "clickhouse",
            "--unknown-funcs",
            "SUM(IF(ISNULL([field]), 0, [field]))",
        ]
    )
    assert stdout == ("sum(if(isNull(my_table.field), 0, my_table.field))\n")
    assert stderr == ""

    stdout, stderr = tool.run(
        [
            "translate",
            "--dialect",
            "clickhouse",
            "--suppress-errors",
        ],
        stdin=("SUM([n1])\n" "AVG([n1])\n" "UNKNOWN_FUNC([n1])"),
    )
    assert stdout.strip() == ("sum(n1)\n" "avg(n1)\n" "#ERROR")
    assert stderr == ""


def test_split(tool):
    stdout, stderr = tool.run(["split", "--diff", "45 BETWEEN [n2] = 123 AND 456"])
    assert stdout == (
        " 0: ▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫\n"
        " 1: ▫▫ BETWEEN ▫▫▫▫▫▫▫▫▫▫ AND ▫▫▫\n"
        " 2: 45         ▫▫▫▫ = ▫▫▫     456\n"
        " 3:            [n2]   123        \n"
    )


def test_graph(tool):
    stdout, stderr = tool.run(
        [
            "graph",
            (
                'IF ([n0]) IN (5, 6) THEN MY_FUNC(NULL, NOT [n2], "qwerty") '
                'ELSEIF [n3] BETWEEN 1 AND 2 THEN "result" '
                'ELSE CASE [n4] WHEN 5 THEN "five" WHEN 6 THEN "seven" ELSE "forty two" END END'
            ),
        ]
    )
    assert "digraph {" in stdout
    assert stderr == ""


def test_dialects(tool):
    stdout, stderr = tool.run(
        [
            "dialects",
        ]
    )
    assert stderr == ""
    assert "DUMMY" in stdout


def test_goto(tool):
    stdout, stderr = tool.run(["goto", "--show", "avg"])
    assert stderr == ""
    assert "functions_aggregation.py" in stdout


def test_slice(tool):
    stdout, stderr = tool.run(
        [
            "slice",
            "--diff",
            "--levels",
            "aggregate,window,toplevel",
            "456 + SUM(FUNC([field])) + RMAX([coeff] * AVG(456 + [qwe] + [rty] + [uio]) - 1)",
        ]
    )
    assert (
        stdout.strip()
        == (
            "toplevel       ▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫ + RMAX(▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫)\n"
            "window         456 + SUM(▫▫▫▫▫▫▫▫▫▫▫▫▫)        ▫▫▫▫▫▫▫ * AVG(▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫▫) - 1 \n"
            "aggregate                FUNC([field])         [coeff]       456 + [qwe] + [rty] + [uio]      \n"
        ).strip()
    )
