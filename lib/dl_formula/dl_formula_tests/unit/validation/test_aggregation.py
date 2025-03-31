from __future__ import annotations


import pytest

from dl_formula.core import (
    exc,
    nodes,
)
from dl_formula.inspect.env import InspectionEnvironment
from dl_formula.shortcuts import n
from dl_formula.validation.aggregation import AggregationChecker
from dl_formula.validation.env import ValidationEnvironment
from dl_formula.validation.validator import validate


def get_dims() -> list[nodes.FormulaItem]:
    return [
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
    ]


def validate_aggregations(
    node: nodes.FormulaItem,
    env: ValidationEnvironment,
    dimensions: list[nodes.FormulaItem],
    collect_errors: bool = False,
) -> None:
    validate(
        node,
        env=env,
        checkers=[AggregationChecker(valid_env=env, inspect_env=InspectionEnvironment(), global_dimensions=dimensions)],
        collect_errors=collect_errors,
    )


def test_no_errors():
    dims = get_dims()
    dim1, dim2 = dims
    env = ValidationEnvironment()

    # not aggregated
    validate_aggregations(
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
        dimensions=dims,
    )

    # aggregated
    validate_aggregations(
        node=nodes.Formula(
            nodes.FuncCall.make(name="sum", args=[nodes.Field.make("Some Field")]),
        ),
        env=env,
        dimensions=dims,
    )
    validate_aggregations(
        node=nodes.Formula(
            nodes.FuncCall.make(name="sum", args=[nodes.LiteralString.make("qwerty")]),
        ),
        env=env,
        dimensions=dims,
    )
    validate_aggregations(
        node=nodes.Formula(
            nodes.FuncCall.make(
                name="sum",
                args=[
                    nodes.ParenthesizedExpr.make(
                        nodes.LiteralString.make("qwerty"),
                    ),
                ],
            ),
        ),
        env=env,
        dimensions=dims,
    )
    validate_aggregations(
        node=nodes.Formula(
            nodes.FuncCall.make(
                name="sum",
                args=[
                    nodes.FuncCall.make(
                        name="+",
                        args=[
                            nodes.Field.make("Barley Field"),
                            nodes.Field.make("Other Field"),
                        ],
                    ),
                ],
            ),
        ),
        env=env,
        dimensions=dims,
    )
    validate_aggregations(
        node=nodes.Formula(
            nodes.FuncCall.make(
                name="sum",
                args=[
                    nodes.FuncCall.make(
                        name="+",
                        args=[
                            dim1,
                            nodes.Field.make("Other Field"),
                        ],
                    ),
                ],
            ),
        ),
        env=env,
        dimensions=dims,
    )
    validate_aggregations(
        node=nodes.Formula(
            nodes.FuncCall.make(
                name="sum",
                args=[
                    nodes.FuncCall.make(
                        name="+",
                        args=[
                            dim1,
                            dim2,
                        ],
                    ),
                ],
            ),
        ),
        env=env,
        dimensions=dims,
    )
    validate_aggregations(
        node=nodes.Formula(
            nodes.FuncCall.make(
                name="sum",
                args=[
                    nodes.FuncCall.make(
                        name="+",
                        args=[
                            dim2,
                            nodes.LiteralFloat.make(1.1),
                        ],
                    ),
                ],
            ),
        ),
        env=env,
        dimensions=dims,
    )

    # nested aggregated
    validate_aggregations(
        node=nodes.Formula(
            nodes.FuncCall.make(
                name="func",
                args=[
                    nodes.FuncCall.make(
                        name="sum",
                        args=[
                            nodes.FuncCall.make(
                                name="+",
                                args=[
                                    dim2,
                                    nodes.LiteralFloat.make(1.1),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
        ),
        env=env,
        dimensions=dims,
    )
    validate_aggregations(
        node=nodes.Formula(
            nodes.FuncCall.make(
                name="func",
                args=[
                    nodes.FuncCall.make(
                        name="sum",
                        args=[
                            nodes.FuncCall.make(
                                name="+",
                                args=[
                                    dim2,
                                    nodes.LiteralFloat.make(1.1),
                                ],
                            ),
                        ],
                    ),
                    dim2,
                ],
            ),
        ),
        env=env,
        dimensions=dims,
    )
    validate_aggregations(
        node=nodes.Formula(
            nodes.FuncCall.make(
                name="func",
                args=[
                    nodes.LiteralBoolean.make(True),
                    nodes.FuncCall.make(
                        name="sum",
                        args=[
                            nodes.FuncCall.make(
                                name="+",
                                args=[
                                    dim2,
                                    nodes.LiteralFloat.make(1.1),
                                ],
                            ),
                        ],
                    ),
                ],
            ),
        ),
        env=env,
        dimensions=dims,
    )


def test_double_aggregation():
    dims = get_dims()
    env = ValidationEnvironment()

    with pytest.raises(exc.ValidationError) as exc_info:
        validate_aggregations(
            nodes.Formula(
                nodes.FuncCall.make(
                    name="+",
                    args=[
                        nodes.LiteralInteger.make(8),
                        nodes.FuncCall.make(
                            name="sum",
                            args=[
                                nodes.FuncCall.make(
                                    name="avg",
                                    args=[
                                        nodes.Field.make("Barley Field"),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
            ),
            env=env,
            dimensions=dims,
        )
    assert exc_info.value.errors[0].code == exc.DoubleAggregationError.default_code


def test_window_sum_is_not_aggregation():
    # here an avg aggregation is wrapped into a sum window function - should not raise an error
    dims = get_dims()
    env = ValidationEnvironment()

    validate_aggregations(
        nodes.Formula(
            nodes.FuncCall.make(
                name="+",
                args=[
                    nodes.LiteralInteger.make(8),
                    nodes.WindowFuncCall.make(
                        name="sum",
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
            ),
        ),
        env=env,
        dimensions=dims,
    )
    assert True  # exception was not raised from the previous statement


def test_inconsistent_aggregation():
    dims = get_dims()
    dim1, dim2 = dims
    env = ValidationEnvironment()

    with pytest.raises(exc.ValidationError) as exc_info:
        validate_aggregations(
            node=nodes.Formula(
                nodes.FuncCall.make(
                    name="+",
                    args=[
                        nodes.FuncCall.make(name="sum", args=[nodes.Field.make("Some Field")]),
                        nodes.Field.make("Rye Field"),
                    ],
                ),
            ),
            env=env,
            dimensions=dims,
        )
    assert exc_info.value.errors[0].code == exc.InconsistentAggregationError.default_code

    with pytest.raises(exc.ValidationError) as exc_info:
        validate_aggregations(
            node=nodes.Formula(
                nodes.FuncCall.make(
                    name="+",
                    args=[
                        nodes.FuncCall.make(name="sum", args=[nodes.Field.make("Some Field")]),
                        nodes.FuncCall.make(name="some_func", args=[nodes.Field.make("Rye Field")]),
                        dim1,
                    ],
                ),
            ),
            env=env,
            dimensions=dims,
        )
    assert exc_info.value.errors[0].code == exc.InconsistentAggregationError.default_code


def test_window_func_of_agg_with_dimensions():
    # check window function that has aggregated and not aggregated children:
    # 1) aggregation func as an argument
    # 2) non-aggregated expression as a dimension
    # this should not raise an inconsistent agg error
    dims = get_dims()
    env = ValidationEnvironment()

    validate_aggregations(
        node=nodes.Formula(
            nodes.WindowFuncCall.make(
                name="rank",
                args=[
                    nodes.FuncCall.make(name="sum", args=[nodes.Field.make("Some Field")]),
                ],
                grouping=nodes.WindowGroupingWithin.make(dim_list=[*dims]),
            ),
        ),
        env=env,
        dimensions=dims,
    )
    assert True  # exception was not raised from the previous statement


def test_multiple_errors():
    dims = get_dims()
    env = ValidationEnvironment()

    def _check():
        with pytest.raises(exc.ValidationError) as exc_info:
            validate_aggregations(
                n.formula(
                    n.func.DO_SOMETHING(
                        n.func.SUM(n.func.AVG(n.field("Electromagnetic Field"))),  # 1. double aggregation
                        n.func.MAX(n.func.MIN(n.field("Field of Gold"))),  # 2. double aggregation
                        # repeat an expr that was already checked
                        n.func.SUM(n.func.AVG(n.field("Electromagnetic Field"))),  # 3. double aggregation (from cache)
                        n.func.MIN(n.field("Field of Gold")),
                        n.field("Field of Gold"),  # 4. inconsistent aggregation
                    ),
                ),
                env=env,
                collect_errors=True,
                dimensions=dims,
            )

        assert len(exc_info.value.errors) == 4  # see above
        assert {err.code for err in exc_info.value.errors} == {
            exc.InconsistentAggregationError.default_code,
            exc.DoubleAggregationError.default_code,
        }

    _check()
    # and now check again re-using the caches
    _check()
