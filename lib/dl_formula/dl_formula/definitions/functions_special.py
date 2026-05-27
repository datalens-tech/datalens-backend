from __future__ import annotations

from dl_formula.core.datatype import DataType
from dl_formula.definitions.args import ArgTypeSequence
from dl_formula.definitions.base import (
    Function,
    TranslationVariant,
)
from dl_formula.definitions.scope import Scope
from dl_formula.definitions.type_strategy import Fixed

V = TranslationVariant.make


class SpecialFunction(Function):
    scopes = Function.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED


class FuncMakeNan(SpecialFunction):
    name = "_make_nan"
    arg_cnt = 0
    argument_types = [ArgTypeSequence([])]
    return_type = Fixed(DataType.FLOAT)


DEFINITIONS_SPECIAL = [
    # _make_nan
    FuncMakeNan,
]
