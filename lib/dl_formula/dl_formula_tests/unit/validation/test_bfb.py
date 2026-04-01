from __future__ import annotations

import pytest

from dl_formula.core import (
    exc,
    nodes,
)
from dl_formula.validation.bfb import BFBChecker
from dl_formula.validation.env import ValidationEnvironment
from dl_formula.validation.validator import validate


def validate_bfb(
    node: nodes.FormulaItem,
    env: ValidationEnvironment,
    field_ids: frozenset[str],
    collect_errors: bool = False,
) -> None:
    validate(
        node,
        env=env,
        checkers=[BFBChecker(field_ids=field_ids)],
        collect_errors=collect_errors,
    )


def test_no_bfb():
    env = ValidationEnvironment()
    field_ids = frozenset({"Dim Field", "Some Field"})

    # simple expression
    validate_bfb(
        node=nodes.Formula(
            nodes.FuncCall.make(
                name="+",
                args=[
                    nodes.Field.make("Dim Field"),
                    nodes.LiteralInteger.make(10),
                ],
            ),
        ),
        env=env,
        field_ids=field_ids,
    )

    # nested function calls
    validate_bfb(
        node=nodes.Formula(
            nodes.FuncCall.make(
                name="sum",
                args=[
                    nodes.FuncCall.make(
                        name="+",
                        args=[
                            nodes.Field.make("Dim Field"),
                            nodes.Field.make("Some Field"),
                        ],
                    ),
                ],
            ),
        ),
        env=env,
        field_ids=field_ids,
    )


def test_correct_bfb_top_level():
    env = ValidationEnvironment()
    field_ids = frozenset({"Dim Field", "Some Field"})

    validate_bfb(
        node=nodes.Formula(
            nodes.FuncCall.make(
                name="sum",
                args=[nodes.Field.make("Some Field")],
                before_filter_by=nodes.BeforeFilterBy.make(field_names={"Dim Field"}),
            ),
        ),
        env=env,
        field_ids=field_ids,
    )


def test_correct_bfb_nested():
    env = ValidationEnvironment()
    field_ids = frozenset({"Dim Field", "Some Field"})

    validate_bfb(
        node=nodes.Formula(
            nodes.FuncCall.make(
                name="+",
                args=[
                    nodes.LiteralInteger.make(100),
                    nodes.FuncCall.make(
                        name="sum",
                        args=[nodes.Field.make("Some Field")],
                        before_filter_by=nodes.BeforeFilterBy.make(field_names={"Dim Field"}),
                    ),
                ],
            ),
        ),
        env=env,
        field_ids=field_ids,
    )


def test_unknown_bfb_top_level():
    env = ValidationEnvironment()
    field_ids = frozenset({"Dim Field", "Some Field"})

    with pytest.raises(exc.ValidationError) as exc_info:
        validate_bfb(
            node=nodes.Formula(
                nodes.FuncCall.make(
                    name="sum",
                    args=[nodes.Field.make("Some Field")],
                    before_filter_by=nodes.BeforeFilterBy.make(field_names={"Unknown Field"}),
                ),
            ),
            env=env,
            field_ids=field_ids,
        )
    assert exc_info.value.errors[0].code == exc.UnknownBFBFieldError.default_code


def test_unknown_bfb_nested():
    env = ValidationEnvironment()
    field_ids = frozenset({"Dim Field", "Some Field"})

    with pytest.raises(exc.ValidationError) as exc_info:
        validate_bfb(
            node=nodes.Formula(
                nodes.FuncCall.make(
                    name="+",
                    args=[
                        nodes.LiteralInteger.make(8),
                        nodes.FuncCall.make(
                            name="avg",
                            args=[nodes.Field.make("Some Field")],
                            before_filter_by=nodes.BeforeFilterBy.make(field_names={"Barley Field"}),
                        ),
                    ],
                ),
            ),
            env=env,
            field_ids=field_ids,
        )
    assert exc_info.value.errors[0].code == exc.UnknownBFBFieldError.default_code


def test_multiple_unknown_bfbs():
    env = ValidationEnvironment()
    field_ids = frozenset({"Dim Field", "Some Field"})

    with pytest.raises(exc.ValidationError) as exc_info:
        validate_bfb(
            node=nodes.Formula(
                nodes.FuncCall.make(
                    name="+",
                    args=[
                        nodes.FuncCall.make(
                            name="sum",
                            args=[nodes.Field.make("Dim Field")],
                            before_filter_by=nodes.BeforeFilterBy.make(field_names={"Rye Field"}),
                        ),
                        nodes.FuncCall.make(
                            name="avg",
                            args=[nodes.Field.make("Some Field")],
                            before_filter_by=nodes.BeforeFilterBy.make(field_names={"Barley Field"}),
                        ),
                    ],
                ),
            ),
            env=env,
            field_ids=field_ids,
            collect_errors=True,
        )
    assert len(exc_info.value.errors) == 2
    for error in exc_info.value.errors:
        assert error.code == exc.UnknownBFBFieldError.default_code
