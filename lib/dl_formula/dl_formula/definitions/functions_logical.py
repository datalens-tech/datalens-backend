from __future__ import annotations

from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import StandardDialect as D
from dl_formula.definitions.args import ArgTypeSequence
from dl_formula.definitions.base import (
    Function,
    TranslationVariant,
    TranslationVariantWrapped,
)
from dl_formula.definitions.flags import ContextFlag
from dl_formula.definitions.scope import Scope
from dl_formula.definitions.type_strategy import (
    CaseTypeStrategy,
    Fixed,
    FromArgs,
    IfTypeStrategy,
)
from dl_formula.shortcuts import n


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


class LogicalFunction(Function):
    pass


class FuncIsnull(LogicalFunction):
    name = "isnull"
    arg_cnt = 1
    arg_names = ["expression"]
    variants = [
        V(D.DUMMY, lambda x: x.is_(None)),
    ]
    return_type = Fixed(DataType.BOOLEAN)
    return_flags = ContextFlag.IS_CONDITION


class FuncIfnull(LogicalFunction):
    name = "ifnull"
    arg_cnt = 2
    arg_names = ["check_value", "alt_value"]
    return_type = FromArgs()


class FuncIsnan(LogicalFunction):
    name = "isnan"
    scopes = LogicalFunction.scopes & ~Scope.DOCUMENTED  # FIXME: add to doc
    arg_cnt = 1
    arg_names = ["expression"]
    return_type = Fixed(DataType.BOOLEAN)


class FuncIfnan(LogicalFunction):
    name = "ifnan"
    scopes = LogicalFunction.scopes & ~Scope.DOCUMENTED  # FIXME: add to doc
    arg_cnt = 2
    arg_names = ["check_value", "alt_value"]
    return_type = FromArgs()


class FuncZn(LogicalFunction):
    name = "zn"
    arg_cnt = 1
    arg_names = ["expression"]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER]),
        ArgTypeSequence([DataType.FLOAT]),
    ]
    return_type = FromArgs()


class FuncIfBase(LogicalFunction):
    scopes = LogicalFunction.scopes & ~Scope.DOCUMENTED  # Documented in _if_block_
    variants = [VW(D.DUMMY, n.func._if_block_)]
    # Disable postprocessing of args so that it can be done when _if_block_ is being applied.
    # Otherwise we will lose the original context flags of these arguments
    postprocess_args = False
    return_type = IfTypeStrategy()


class FuncIf(FuncIfBase):
    name = "if"
    arg_cnt = None


class FuncIif3Legacy(FuncIfBase):
    name = "iif"
    arg_cnt = 3
    return_flags = ContextFlag.DEPRECATED


class FuncCase(LogicalFunction):
    name = "case"
    arg_cnt = None
    scopes = LogicalFunction.scopes & ~Scope.DOCUMENTED  # Documented in _case_block_
    variants = [VW(D.DUMMY, n.func._case_block_)]  # noqa
    return_type = CaseTypeStrategy()


DEFINITIONS_LOGICAL = [
    # case
    FuncCase,
    # if
    FuncIf,
    # ifnan
    FuncIfnan,
    # ifnull
    FuncIfnull,
    # iif
    FuncIif3Legacy,
    # isnan
    FuncIsnan,
    # isnull
    FuncIsnull,
    # zn
    FuncZn,
]
