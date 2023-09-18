from __future__ import annotations

import sqlalchemy as sa

from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import StandardDialect as D
from dl_formula.definitions.args import (
    ArgFlagSequence,
    ArgTypeSequence,
)
from dl_formula.definitions.base import (
    MultiVariantTranslation,
    TranslationVariant,
)
from dl_formula.definitions.flags import ContextFlag
from dl_formula.definitions.type_strategy import (
    Fixed,
    FromArgs,
)

V = TranslationVariant.make


class Unary(MultiVariantTranslation):
    arg_cnt = 1
    is_function = False


class UnaryNot(Unary):
    name = "not"
    return_type = Fixed(DataType.BOOLEAN)
    return_flags = int(ContextFlag.IS_CONDITION)


class UnaryNotBool(UnaryNot):
    variants = [V(D.DUMMY | D.SQLITE, sa.not_)]
    argument_types = [
        ArgTypeSequence([DataType.BOOLEAN]),
    ]
    argument_flags = ArgFlagSequence([ContextFlag.REQ_CONDITION])


class UnaryNotNumbers(UnaryNot):
    variants = [V(D.DUMMY | D.SQLITE, lambda x: x == 0)]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER]),
        ArgTypeSequence([DataType.FLOAT]),
    ]


class UnaryNotStringGeo(UnaryNot):
    variants = [
        V(D.DUMMY, lambda x: x == ""),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
        ArgTypeSequence([DataType.GEOPOINT]),
        ArgTypeSequence([DataType.GEOPOLYGON]),
    ]


class UnaryNotDateDatetime(UnaryNot):
    variants = [
        V(D.DUMMY, lambda x: sa.literal(False)),
    ]
    argument_types = [
        ArgTypeSequence([DataType.DATE]),
        ArgTypeSequence([DataType.DATETIME]),
        ArgTypeSequence([DataType.GENERICDATETIME]),
    ]
    return_flags = 0


class UnaryNegate(Unary):
    name = "neg"
    variants = [V(D.DUMMY | D.SQLITE, lambda x: -x)]
    return_type = FromArgs()
    argument_types = [
        ArgTypeSequence([DataType.INTEGER]),
        ArgTypeSequence([DataType.FLOAT]),
    ]


class UnaryIsOpBase(Unary):
    return_type = Fixed(DataType.BOOLEAN)
    return_flags = int(ContextFlag.IS_CONDITION)


class UnaryIsTrue(UnaryIsOpBase):
    name = "istrue"
    arg_names = ["value"]


class UnaryIsTrueStringGeo(UnaryIsTrue):
    variants = [
        # TODO?: use `n.func.NOT`?
        V(D.DUMMY, lambda x: x != ""),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
        ArgTypeSequence([DataType.GEOPOINT]),
        ArgTypeSequence([DataType.GEOPOLYGON]),
    ]


class UnaryIsTrueNumbers(UnaryIsTrue):
    # TODO?: use `n.func.BOOL`?
    variants = [V(D.DUMMY | D.SQLITE, lambda x: x != 0.0)]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]


class UnaryIsTrueDateTime(UnaryIsTrue):
    # TODO?: use `n.func.NOT`?
    variants = [
        V(D.DUMMY, lambda x: sa.literal(True)),
    ]
    argument_types = [
        ArgTypeSequence([DataType.DATE]),
        ArgTypeSequence([DataType.DATETIME]),
        ArgTypeSequence([DataType.GENERICDATETIME]),
    ]
    return_flags = 0


class UnaryIsTrueBoolean(UnaryIsTrue):
    variants = [
        V(D.DUMMY, lambda x: x.is_(True)),
    ]
    argument_types = [
        ArgTypeSequence([DataType.BOOLEAN]),
    ]


class UnaryIsFalse(UnaryIsOpBase):
    name = "isfalse"
    arg_names = ["value"]


class UnaryIsFalseStringGeo(UnaryIsFalse):
    variants = [
        # TODO?: use `n.func.NOT`?
        V(D.DUMMY, lambda x: x == ""),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING]),
        ArgTypeSequence([DataType.GEOPOINT]),
        ArgTypeSequence([DataType.GEOPOLYGON]),
    ]


class UnaryIsFalseNumbers(UnaryIsFalse):
    variants = [V(D.DUMMY | D.SQLITE, lambda x: x == 0.0)]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT]),
    ]


class UnaryIsFalseDateTime(UnaryIsFalse):
    variants = [
        V(D.DUMMY, lambda x: sa.literal(False)),
    ]
    argument_types = [
        ArgTypeSequence([DataType.DATE]),
        ArgTypeSequence([DataType.DATETIME]),
        ArgTypeSequence([DataType.GENERICDATETIME]),
    ]
    return_flags = 0


class UnaryIsFalseBoolean(UnaryIsFalse):
    variants = [
        V(D.DUMMY, lambda x: x.is_(False)),
    ]
    argument_types = [
        ArgTypeSequence([DataType.BOOLEAN]),
    ]


DEFINITIONS_UNARY = [
    # isfalse
    UnaryIsFalseStringGeo,
    UnaryIsFalseNumbers,
    UnaryIsFalseDateTime,
    UnaryIsFalseBoolean,
    # istrue
    UnaryIsTrueStringGeo,
    UnaryIsTrueNumbers,
    UnaryIsTrueDateTime,
    UnaryIsTrueBoolean,
    # neg
    UnaryNegate,
    # not
    UnaryNotBool,
    UnaryNotNumbers,
    UnaryNotStringGeo,
    UnaryNotDateDatetime,
]
