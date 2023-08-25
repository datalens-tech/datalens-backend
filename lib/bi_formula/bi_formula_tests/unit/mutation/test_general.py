from __future__ import annotations

from bi_formula.mutation.general import (
    IgnoreParenthesisWrapperMutation,
    ConvertBlocksToFunctionsMutation,
    OptimizeConstComparisonMutation,
    OptimizeConstAndOrMutation,
)
from bi_formula.mutation.mutation import apply_mutations
from bi_formula.shortcuts import n


def test_ignore_parenthesis_wrapper_mutation():
    formula_obj = n.formula(
        n.func.my_func(args=[n.p(n.func.FUNC(args=[]))])
    )
    formula_obj = apply_mutations(formula_obj, mutations=[
        IgnoreParenthesisWrapperMutation(),
    ])
    assert formula_obj == n.formula(
        n.func.my_func(args=[n.func.FUNC(args=[])])
    )


def test_convert_blocks_to_functions_mutation():
    formula_obj = n.formula(
        n.if_(n.field('this')).then(n.field('that')).else_(n.field('smth_else'))
    )
    formula_obj = apply_mutations(formula_obj, mutations=[
        ConvertBlocksToFunctionsMutation(),
    ])
    assert formula_obj == n.formula(
        n.func.IF(args=[n.field('this'), n.field('that'), n.field('smth_else')])
    )


def test_optimize_const_comparison_mutation():
    formula_obj = n.formula(
        n.binary('==', left=n.lit(3), right=n.lit(3))
    )
    formula_obj = apply_mutations(formula_obj, mutations=[
        OptimizeConstComparisonMutation(),
    ])
    assert formula_obj == n.formula(n.lit(True))

    formula_obj = n.formula(
        n.binary('==', left=n.lit(2), right=n.lit(3))
    )
    formula_obj = apply_mutations(formula_obj, mutations=[
        OptimizeConstComparisonMutation(),
    ])
    assert formula_obj == n.formula(n.lit(False))

    formula_obj = n.formula(
        n.binary('!=', left=n.lit(2), right=n.lit(2))
    )
    formula_obj = apply_mutations(formula_obj, mutations=[
        OptimizeConstComparisonMutation(),
    ])
    assert formula_obj == n.formula(n.lit(False))

    formula_obj = n.formula(
        n.binary('!=', left=n.lit(2), right=n.lit(3))
    )
    formula_obj = apply_mutations(formula_obj, mutations=[
        OptimizeConstComparisonMutation(),
    ])
    assert formula_obj == n.formula(n.lit(True))


def test_optimize_const_and_or_mutation():
    formula_obj = n.formula(
        n.binary('and', left=n.lit(True), right=n.field('my field'))
    )
    formula_obj = apply_mutations(formula_obj, mutations=[
        OptimizeConstAndOrMutation(),
    ])
    assert formula_obj == n.formula(n.field('my field'))

    formula_obj = n.formula(
        n.binary('and', left=n.lit(False), right=n.field('my field'))
    )
    formula_obj = apply_mutations(formula_obj, mutations=[
        OptimizeConstAndOrMutation(),
    ])
    assert formula_obj == n.formula(n.lit(False))

    formula_obj = n.formula(
        n.binary('or', left=n.field('my field'), right=n.lit(True))
    )
    formula_obj = apply_mutations(formula_obj, mutations=[
        OptimizeConstAndOrMutation(),
    ])
    assert formula_obj == n.formula(n.lit(True))

    formula_obj = n.formula(
        n.binary('or', left=n.field('my field'), right=n.lit(False))
    )
    formula_obj = apply_mutations(formula_obj, mutations=[
        OptimizeConstAndOrMutation(),
    ])
    assert formula_obj == n.formula(n.field('my field'))
