from __future__ import annotations

from bi_formula.core.datatype import DataType
from bi_formula.definitions.scope import Scope
from bi_formula.definitions.args import ArgTypeSequence
from bi_formula.definitions.type_strategy import Fixed
from bi_formula.definitions.base import (
    TranslationVariant,
    Function,
)


V = TranslationVariant.make


class SpecialFunction(Function):
    scopes = Function.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED


class FuncMakeNan(SpecialFunction):
    name = '_make_nan'
    arg_cnt = 0
    argument_types = [ArgTypeSequence([])]
    return_type = Fixed(DataType.FLOAT)


DEFINITIONS_SPECIAL = [
    # _make_nan
    FuncMakeNan,
]
