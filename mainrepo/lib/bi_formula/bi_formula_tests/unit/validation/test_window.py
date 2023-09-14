from __future__ import annotations

from typing import Tuple

import pytest

from bi_formula.core import (
    exc,
    nodes,
)
from bi_formula.inspect.env import InspectionEnvironment
from bi_formula.validation.env import ValidationEnvironment
from bi_formula.validation.validator import validate
from bi_formula.validation.window import WindowFunctionChecker


def get_dims() -> Tuple[nodes.FormulaItem, nodes.FormulaItem]:
    return (
        nodes.Field.make("Dim Field"),
        nodes.FuncCall.make(
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
        ),
    )


def validate_window_functions(
    node: nodes.FormulaItem, env: ValidationEnvironment, collect_errors: bool = False
) -> None:
    validate(
        node,
        env=env,
        checkers=[WindowFunctionChecker(inspect_env=InspectionEnvironment())],
        collect_errors=collect_errors,
    )


def test_no_errors():
    dim1, dim2 = get_dims()
    env = ValidationEnvironment()

    # no window function
    validate_window_functions(
        node=nodes.Formula(
            nodes.FuncCall.make(
                name="+",
                args=[
                    dim2,
                    nodes.LiteralFloat.make(1.1),
                ],
            ),
        ),
        env=env,
    )
    validate_window_functions(
        node=nodes.Formula(
            nodes.FuncCall.make(name="sum", args=[nodes.LiteralString.make("qwerty")]),
        ),
        env=env,
    )

    # with window function
    validate_window_functions(
        node=nodes.Formula(
            nodes.WindowFuncCall.make(
                name="rank",
                args=[
                    nodes.FuncCall.make(name="sum", args=[nodes.LiteralString.make("qwerty")]),
                ],
                grouping=nodes.WindowGroupingTotal(),
            ),
        ),
        env=env,
    )
    validate_window_functions(
        node=nodes.Formula(
            nodes.FuncCall.make(
                name="+",
                args=[
                    nodes.WindowFuncCall.make(
                        name="rank",
                        args=[
                            nodes.FuncCall.make(name="sum", args=[nodes.LiteralString.make("qwerty")]),
                        ],
                        grouping=nodes.WindowGroupingTotal(),
                    ),
                    nodes.WindowFuncCall.make(
                        name="rank_dense",
                        args=[
                            nodes.FuncCall.make(name="sum", args=[nodes.LiteralString.make("qwerty")]),
                        ],
                        grouping=nodes.WindowGroupingTotal(),
                    ),
                ],
            ),
        ),
        env=env,
    )


def test_nested_window_functions():
    env = ValidationEnvironment()

    with pytest.raises(exc.ValidationError) as exc_info:
        validate_window_functions(
            nodes.Formula(
                nodes.FuncCall.make(
                    name="+",
                    args=[
                        nodes.LiteralInteger.make(8),
                        nodes.WindowFuncCall.make(
                            name="rank",
                            args=[
                                nodes.WindowFuncCall.make(
                                    name="rank_dense",
                                    args=[
                                        nodes.FuncCall.make(
                                            name="avg",
                                            args=[
                                                nodes.Field.make("Barley Field"),
                                            ],
                                        ),
                                    ],
                                    grouping=nodes.WindowGroupingTotal(),
                                ),
                            ],
                            grouping=nodes.WindowGroupingTotal(),
                        ),
                    ],
                ),
            ),
            env=env,
        )
    assert exc_info.value.errors[0].code == exc.NestedWindowFunctionError.default_code


def test_window_function_without_aggregation():
    env = ValidationEnvironment()

    with pytest.raises(exc.ValidationError) as exc_info:
        validate_window_functions(
            nodes.Formula(
                nodes.FuncCall.make(
                    name="+",
                    args=[
                        nodes.LiteralInteger.make(8),
                        nodes.WindowFuncCall.make(
                            name="rank",
                            args=[
                                nodes.Field.make("Barley Field"),
                            ],
                            grouping=nodes.WindowGroupingTotal(),
                        ),
                    ],
                ),
            ),
            env=env,
        )
    assert exc_info.value.errors[0].code == exc.WindowFunctionWOAggregationError.default_code
