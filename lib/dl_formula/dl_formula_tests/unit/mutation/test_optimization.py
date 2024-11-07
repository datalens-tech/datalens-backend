from dl_formula.mutation.mutation import apply_mutations
from dl_formula.mutation.optimization import (
    OptimizeAndOrComparisonMutation,
    OptimizeBinaryOperatorComparisonMutation,
    OptimizeConstAndOrMutation,
    OptimizeConstComparisonMutation,
    OptimizeConstFuncMutation,
    OptimizeConstMathOperatorMutation,
)
from dl_formula.shortcuts import n


def test_optimize_const_comparison_mutation():
    formula_obj = n.formula(n.binary("==", left=n.lit(3), right=n.lit(3)))
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstComparisonMutation(),
        ],
    )
    assert formula_obj == n.formula(n.lit(True))

    formula_obj = n.formula(n.binary("==", left=n.lit(2), right=n.lit(3)))
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstComparisonMutation(),
        ],
    )
    assert formula_obj == n.formula(n.lit(False))

    formula_obj = n.formula(n.binary("!=", left=n.lit(2), right=n.lit(2)))
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstComparisonMutation(),
        ],
    )
    assert formula_obj == n.formula(n.lit(False))

    formula_obj = n.formula(n.binary("!=", left=n.lit(2), right=n.lit(3)))
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstComparisonMutation(),
        ],
    )
    assert formula_obj == n.formula(n.lit(True))


def test_optimize_const_math_operator_mutation():
    formula_obj = n.formula(n.binary("+", left=n.lit(2.0), right=n.lit(3.0)))
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstMathOperatorMutation(),
        ],
    )
    assert formula_obj == n.formula(n.lit(5.0))

    formula_obj = n.formula(n.binary("-", left=n.lit(2.0), right=n.lit(3)))
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstMathOperatorMutation(),
        ],
    )
    assert formula_obj == n.formula(n.lit(-1.0))

    formula_obj = n.formula(n.binary("*", left=n.lit(2), right=n.lit(3)))
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstMathOperatorMutation(),
        ],
    )
    assert formula_obj == n.formula(n.lit(6))

    formula_obj = n.formula(n.binary("/", left=n.lit(2), right=n.lit(5)))
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstMathOperatorMutation(),
        ],
    )
    assert formula_obj == n.formula(n.lit(0.4))

    # special case: avoid division by zero and don't mutate the formula
    formula_obj = n.formula(n.binary("/", left=n.lit(1), right=n.lit(0)))
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstMathOperatorMutation(),
        ],
    )
    assert formula_obj == n.formula(n.binary("/", left=n.lit(1), right=n.lit(0)))


def test_optimize_binary_operator_comparison_mutation():
    formula_obj = n.formula(
        n.binary(
            "==",
            left=n.binary("<", n.field("a"), n.field("b")),
            right=n.lit(1),
        ),
    )
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeBinaryOperatorComparisonMutation(),
        ],
    )
    assert formula_obj == n.formula(n.binary("<", n.field("a"), n.field("b")))

    formula_obj = n.formula(
        n.binary(
            "==",
            left=n.binary(">", n.field("a"), n.field("b")),
            right=n.lit(False),
        ),
    )
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeBinaryOperatorComparisonMutation(),
        ],
    )
    assert formula_obj == n.formula(n.binary("<=", n.field("a"), n.field("b")))

    formula_obj = n.formula(
        n.binary(
            "!=",
            left=n.binary("<=", n.field("a"), n.field("b")),
            right=n.lit(True),
        ),
    )
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeBinaryOperatorComparisonMutation(),
        ],
    )
    assert formula_obj == n.formula(n.binary(">", n.field("a"), n.field("b")))

    formula_obj = n.formula(
        n.binary(
            "!=",
            left=n.binary(">=", n.field("a"), n.field("b")),
            right=n.lit(0),
        ),
    )
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeBinaryOperatorComparisonMutation(),
        ],
    )
    assert formula_obj == n.formula(n.binary(">=", n.field("a"), n.field("b")))

    formula_obj = n.formula(
        n.binary(
            "and",
            n.binary(
                "==",
                left=n.binary("==", n.field("a"), n.field("b")),
                right=n.lit(False),
            ),
            n.binary(
                "!=",
                left=n.binary("!=", n.field("c"), n.field("d")),
                right=n.lit(1),
            ),
        ),
    )
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeBinaryOperatorComparisonMutation(),
        ],
    )
    assert formula_obj == n.formula(
        n.binary(
            "and",
            n.binary("!=", n.field("a"), n.field("b")),
            n.binary("==", n.field("c"), n.field("d")),
        )
    )

    formula_obj = n.formula(
        n.binary(
            "or",
            n.binary(
                "==",
                left=n.binary("in", n.field("a"), n.lit([1, 2, 3])),
                right=n.lit(False),
            ),
            n.binary(
                "!=",
                left=n.binary("notin", n.field("b"), n.lit([4, 5, 6])),
                right=n.lit(1),
            ),
        ),
    )
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeBinaryOperatorComparisonMutation(),
        ],
    )
    assert formula_obj == n.formula(
        n.binary(
            "or",
            n.binary("notin", n.field("a"), n.lit([1, 2, 3])),
            n.binary("in", n.field("b"), n.lit([4, 5, 6])),
        )
    )

def test_optimize_and_or_comparison_mutation():
    formula_obj = n.formula(
        n.binary(
            "==",
            left=n.binary("or", n.field("a"), n.field("b")),
            right=n.lit(1),
        ),
    )
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeAndOrComparisonMutation(),
        ],
    )
    assert formula_obj == n.formula(n.binary("or", n.field("a"), n.field("b")))

    formula_obj = n.formula(
        n.binary(
            "==",
            left=n.binary("and", n.field("a"), n.field("b")),
            right=n.lit(False),
        ),
    )
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeAndOrComparisonMutation(),
        ],
    )
    assert formula_obj == n.formula(n.unary("not", n.binary("and", n.field("a"), n.field("b"))))

    formula_obj = n.formula(
        n.binary(
            "!=",
            left=n.binary("or", n.field("a"), n.field("b")),
            right=n.lit(True),
        ),
    )
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeAndOrComparisonMutation(),
        ],
    )
    assert formula_obj == n.formula(n.unary("not", n.binary("or", n.field("a"), n.field("b"))))

    formula_obj = n.formula(
        n.binary(
            "!=",
            left=n.binary("and", n.field("a"), n.field("b")),
            right=n.lit(0),
        ),
    )
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeAndOrComparisonMutation(),
        ],
    )
    assert formula_obj == n.formula(n.binary("and", n.field("a"), n.field("b")))


def test_optimize_const_and_or_mutation():
    formula_obj = n.formula(n.binary("and", left=n.lit(True), right=n.field("my field")))
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstAndOrMutation(),
        ],
    )
    assert formula_obj == n.formula(n.field("my field"))

    formula_obj = n.formula(n.binary("and", left=n.lit(False), right=n.field("my field")))
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstAndOrMutation(),
        ],
    )
    assert formula_obj == n.formula(n.lit(False))

    formula_obj = n.formula(n.binary("or", left=n.field("my field"), right=n.lit(True)))
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstAndOrMutation(),
        ],
    )
    assert formula_obj == n.formula(n.lit(True))

    formula_obj = n.formula(n.binary("or", left=n.field("my field"), right=n.lit(False)))
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstAndOrMutation(),
        ],
    )
    assert formula_obj == n.formula(n.field("my field"))


def test_optimize_if_mutation():
    # Check removal of false conditions
    formula_obj = n.formula(
        n.func.IF(
            n.field("cond 1"),
            n.field("then field 1"),
            n.lit(False),
            n.field("then field 2"),
            n.lit(False),
            n.field("then field 3"),
            n.field("cond 4"),
            n.field("then field 4"),
            n.field("else field"),
        )
    )
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstFuncMutation(),
        ],
    )
    assert formula_obj == n.formula(
        n.func.IF(
            n.field("cond 1"),
            n.field("then field 1"),
            n.field("cond 4"),
            n.field("then field 4"),
            n.field("else field"),
        )
    )

    # Check only else
    formula_obj = n.formula(
        n.func.IF(
            n.lit(False),
            n.field("then field 1"),
            n.lit(False),
            n.field("then field 2"),
            n.field("else field"),
        )
    )
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstFuncMutation(),
        ],
    )
    assert formula_obj == n.formula(n.field("else field"))

    # Check true condition
    formula_obj = n.formula(
        n.func.IF(
            n.field("cond 1"),
            n.field("then field 1"),
            n.lit(False),
            n.field("then field 2"),
            n.lit(True),
            n.field("then field 3"),
            n.field("cond 4"),
            n.field("then field 4"),
            n.field("else field"),
        )
    )
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstFuncMutation(),
        ],
    )
    assert formula_obj == n.formula(n.field("then field 3"))


def test_optimize_if_mutation_with_const_comparison():
    # Check false comparison result in removal of a corresponding branch
    formula_obj = n.formula(
        n.func.IF(
            n.binary("==", left=n.lit(2), right=n.lit(3)),
            n.field("then field 1"),
            n.binary("!=", left=n.lit(2), right=n.lit(2)),
            n.field("then field 2"),
            n.field("else field"),
        )
    )
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstComparisonMutation(),
            OptimizeConstFuncMutation(),
        ],
    )
    assert formula_obj == n.formula(n.field("else field"))

    # Check true comparison result in single field result
    formula_obj = n.formula(
        n.func.IF(
            n.field("cond 1"),
            n.field("then field 1"),
            n.binary("!=", left=n.lit(2), right=n.lit(2)),
            n.field("then field 2"),
            n.binary("==", left=n.lit(2), right=n.lit(2)),
            n.field("then field 3"),
            n.field("cond 4"),
            n.field("then field 4"),
            n.field("else field"),
        )
    )
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstComparisonMutation(),
            OptimizeConstFuncMutation(),
        ],
    )
    assert formula_obj == n.formula(n.field("then field 3"))


def test_optimize_case_mutation():
    # Check removal of false conditions
    formula_obj = n.formula(
        n.func.CASE(
            n.lit("what"),
            n.field("when 1"),
            n.field("then field 1"),
            n.lit("qwerty"),
            n.field("then field 2"),
            n.lit("uiop"),
            n.field("then field 3"),
            n.field("when 4"),
            n.field("then field 4"),
            n.field("else field"),
        )
    )
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstFuncMutation(),
        ],
    )
    assert formula_obj == n.formula(
        n.func.CASE(
            n.lit("what"),
            n.field("when 1"),
            n.field("then field 1"),
            n.field("when 4"),
            n.field("then field 4"),
            n.field("else field"),
        )
    )

    # Check only else
    formula_obj = n.formula(
        n.func.CASE(
            n.lit("what"),
            n.lit("qwerty"),
            n.field("then field 1"),
            n.lit("uiop"),
            n.field("then field 2"),
            n.field("else field"),
        )
    )
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstFuncMutation(),
        ],
    )
    assert formula_obj == n.formula(n.field("else field"))

    # Check match
    formula_obj = n.formula(
        n.func.CASE(
            n.lit("what"),
            n.field("when 1"),
            n.field("then field 1"),
            n.lit("qwerty"),
            n.field("then field 2"),
            n.lit("what"),
            n.field("then field 3"),
            n.field("when 4"),
            n.field("then field 4"),
            n.field("else field"),
        )
    )
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            OptimizeConstFuncMutation(),
        ],
    )
    assert formula_obj == n.formula(n.field("then field 3"))
