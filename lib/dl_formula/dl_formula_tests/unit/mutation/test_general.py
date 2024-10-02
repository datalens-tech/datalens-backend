from dl_formula.mutation.general import (
    ConvertBlocksToFunctionsMutation,
    IgnoreParenthesisWrapperMutation,
)
from dl_formula.mutation.mutation import apply_mutations
from dl_formula.shortcuts import n


def test_ignore_parenthesis_wrapper_mutation():
    formula_obj = n.formula(n.func.my_func(args=[n.p(n.func.FUNC(args=[]))]))
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            IgnoreParenthesisWrapperMutation(),
        ],
    )
    assert formula_obj == n.formula(n.func.my_func(args=[n.func.FUNC(args=[])]))


def test_convert_blocks_to_functions_mutation():
    formula_obj = n.formula(n.if_(n.field("this")).then(n.field("that")).else_(n.field("smth_else")))
    formula_obj = apply_mutations(
        formula_obj,
        mutations=[
            ConvertBlocksToFunctionsMutation(),
        ],
    )
    assert formula_obj == n.formula(n.func.IF(args=[n.field("this"), n.field("that"), n.field("smth_else")]))
