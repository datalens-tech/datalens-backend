from __future__ import annotations

import sqlalchemy as sa

from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import StandardDialect as D
from dl_formula.definitions.args import ArgTypeSequence
from dl_formula.definitions.base import (
    MultiVariantTranslation,
    TranslationVariant,
)
from dl_formula.definitions.flags import ContextFlag
from dl_formula.definitions.scope import Scope
from dl_formula.definitions.type_strategy import Fixed


V = TranslationVariant.make


class Ternary(MultiVariantTranslation):
    arg_cnt = 3
    is_function = False


class TernaryBetweenBase(Ternary):
    arg_names = ["value", "low", "high"]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT, DataType.FLOAT]),
        ArgTypeSequence([DataType.DATE, DataType.DATE, DataType.DATE]),
        ArgTypeSequence([DataType.DATETIME, DataType.DATETIME, DataType.DATETIME]),
        ArgTypeSequence([DataType.DATETIMETZ, DataType.DATETIMETZ, DataType.DATETIMETZ]),
        ArgTypeSequence([DataType.GENERICDATETIME, DataType.GENERICDATETIME, DataType.GENERICDATETIME]),
        # TODO later: ArgTypeSequence([DataType.DATETIME, DataType.DATETIMETZ, DataType.DATETIMETZ]),
        ArgTypeSequence([DataType.STRING, DataType.STRING, DataType.STRING]),
    ]
    return_type = Fixed(DataType.BOOLEAN)
    return_flags = ContextFlag.IS_CONDITION


class TernaryBetween(TernaryBetweenBase):
    name = "between"
    variants = [
        V(D.DUMMY | D.SQLITE, lambda a, b, c: a.between(b, c)),
    ]


class TernaryNotBetween(TernaryBetweenBase):
    name = "notbetween"
    scopes = TernaryBetweenBase.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED
    variants = [
        V(D.DUMMY | D.SQLITE, lambda a, b, c: sa.not_(a.between(b, c))),
    ]


DEFINITIONS_TERNARY = [
    # between
    TernaryBetween,
    # notbetween
    TernaryNotBetween,
]
