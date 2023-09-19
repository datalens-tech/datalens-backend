from __future__ import annotations

import functools
from typing import TYPE_CHECKING

import sqlalchemy as sa

from dl_formula.core import exc
from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import StandardDialect as D
from dl_formula.definitions.args import (
    ArgFlagSequence,
    ArgTypeMatcher,
    ArgTypeSequence,
)
from dl_formula.definitions.base import (
    ArgTransformer,
    MultiVariantTranslation,
    TranslationVariant,
    TranslationVariantWrapped,
)
from dl_formula.definitions.flags import ContextFlag
from dl_formula.definitions.literals import (
    literal,
    un_literal,
)
from dl_formula.definitions.scope import Scope
from dl_formula.definitions.type_strategy import (
    Fixed,
    FromArgs,
)
from dl_formula.shortcuts import n


if TYPE_CHECKING:
    from dl_formula.translation.context import TranslationCtx
    from dl_formula.translation.env import TranslationEnvironment


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


def as_bigint(value):
    return sa.cast(value, sa.BIGINT())


class Binary(MultiVariantTranslation):
    arg_cnt = 2
    is_function = False
    arg_names = ["left", "right"]


class BinaryPower(Binary):
    name = "^"
    arg_names = ["base", "power"]
    variants = [VW(D.DUMMY, lambda base, power: n.func.POWER(base, power))]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.FLOAT)


class BinaryMult(Binary):
    name = "*"
    arg_names = ["value_1", "value_2"]


class BinaryMultNumbers(BinaryMult):
    variants = [
        V(D.DUMMY | D.SQLITE, lambda left, right: left * right),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
    ]
    return_type = FromArgs()


class BinaryMultStringConst(BinaryMult):
    variants = [
        V(D.DUMMY | D.SQLITE, lambda text, size: literal(un_literal(text) * un_literal(size))),
    ]
    argument_types = [
        ArgTypeSequence([DataType.CONST_STRING, DataType.CONST_INTEGER]),
        ArgTypeSequence([DataType.CONST_INTEGER, DataType.CONST_STRING]),
    ]
    return_type = Fixed(DataType.CONST_STRING)


class BinaryMultStringNonConst(BinaryMult):
    class BinaryMultStringNonConstArgTransformer(ArgTransformer):
        def transform_args(
            self, env: TranslationEnvironment, args: list[TranslationCtx], arg_types: list[DataType]
        ) -> list[TranslationCtx]:
            """Reorder args so that str is always first and number of repeats is second"""
            if arg_types[0] not in (DataType.CONST_STRING, DataType.STRING):
                args = [args[1], args[0]]
            return args

    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.INTEGER]),
        ArgTypeSequence([DataType.INTEGER, DataType.STRING]),
    ]
    return_type = Fixed(DataType.STRING)
    arg_transformer = BinaryMultStringNonConstArgTransformer()


class BinaryDiv(Binary):
    name = "/"
    return_type = Fixed(DataType.FLOAT)
    arg_names = ["number_1", "number_2"]


class BinaryDivInt(BinaryDiv):
    variants = [
        V(D.DUMMY, lambda x, y: x / y),
    ]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER, DataType.INTEGER]),
    ]


class BinaryDivFloat(BinaryDiv):
    variants = [
        V(D.DUMMY | D.SQLITE, lambda x, y: x / y),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
    ]


class BinaryMod(Binary):
    name = "%"
    arg_names = ["number_1", "number_2"]


class BinaryModInteger(BinaryMod):
    variants = [
        V(D.DUMMY | D.SQLITE, lambda left, right: left % right),
    ]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER, DataType.INTEGER]),
    ]
    return_type = FromArgs()


class BinaryModFloat(BinaryMod):
    variants = [
        V(D.DUMMY, lambda left, right: left % right),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
    ]
    return_type = FromArgs()


class BinaryPlus(Binary):
    name = "+"
    arg_names = ["value_1", "value_2"]


class BinaryPlusNumbers(BinaryPlus):
    variants = [
        V(D.DUMMY | D.SQLITE, lambda left, right: left + right),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
    ]
    return_type = FromArgs()


class BinaryPlusStrings(BinaryPlus):
    variants = [
        # `[str1] + [str2]` is equivalent to `concat([str1], [str2])`
        VW(D.DUMMY | D.SQLITE, n.func.CONCAT),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.STRING]),
    ]
    return_type = Fixed(DataType.STRING)


class BinaryPlusDateInt(BinaryPlus):
    class BinaryPlusDateIntArgTransformer(ArgTransformer):
        def transform_args(
            self, env: TranslationEnvironment, args: list[TranslationCtx], arg_types: list[DataType]
        ) -> list[TranslationCtx]:
            """Reorder args so that date is always first and number of days is second"""
            if arg_types[0] not in (DataType.CONST_DATE, DataType.DATE):
                args = [args[1], args[0]]
            return args

    arg_transformer = BinaryPlusDateIntArgTransformer()
    argument_types = [
        ArgTypeSequence([DataType.DATE, DataType.INTEGER]),
        ArgTypeSequence([DataType.INTEGER, DataType.DATE]),
    ]
    return_type = Fixed(DataType.DATE)


class BinaryPlusArray(BinaryPlus):
    argument_types = [
        ArgTypeSequence([DataType.ARRAY_INT, DataType.ARRAY_INT]),
        ArgTypeSequence([DataType.ARRAY_FLOAT, DataType.ARRAY_FLOAT]),
        ArgTypeSequence([DataType.ARRAY_STR, DataType.ARRAY_STR]),
    ]
    return_type = FromArgs()


class BinaryPlusDateFloat(BinaryPlus):
    class BinaryPlusDateFloatArgTransformer(ArgTransformer):
        def transform_args(
            self, env: TranslationEnvironment, args: list[TranslationCtx], arg_types: list[DataType]
        ) -> list[TranslationCtx]:
            """Reorder args so that date is always first and number of days is second"""
            if arg_types[0] not in (DataType.CONST_DATE, DataType.DATE):
                args = [args[1], args[0]]
            return args

    arg_transformer = BinaryPlusDateFloatArgTransformer()
    argument_types = [
        ArgTypeSequence([DataType.DATE, DataType.FLOAT]),
        ArgTypeSequence([DataType.FLOAT, DataType.DATE]),
    ]
    return_type = Fixed(DataType.DATE)


class BinaryPlusDatetimeNumber(BinaryPlus):
    class BinaryPlusDatetimeNumberArgTransformer(ArgTransformer):
        def transform_args(
            self, env: TranslationEnvironment, args: list[TranslationCtx], arg_types: list[DataType]
        ) -> list[TranslationCtx]:
            """Reorder args so that date is always first and number of days is second"""
            if arg_types[0] not in (DataType.CONST_DATETIME, DataType.DATETIME):
                args = [args[1], args[0]]
            return args

    arg_transformer = BinaryPlusDatetimeNumberArgTransformer()
    argument_types = [
        ArgTypeSequence([DataType.DATETIME, DataType.FLOAT]),
        ArgTypeSequence([DataType.FLOAT, DataType.DATETIME]),
    ]
    return_type = Fixed(DataType.DATETIME)


class BinaryPlusGenericDatetimeNumber(BinaryPlus):
    class BinaryPlusGenericDatetimeNumberArgTransformer(ArgTransformer):
        def transform_args(
            self, env: TranslationEnvironment, args: list[TranslationCtx], arg_types: list[DataType]
        ) -> list[TranslationCtx]:
            """Reorder args so that date is always first and number of days is second"""
            if arg_types[0] not in (DataType.CONST_GENERICDATETIME, DataType.GENERICDATETIME):
                args = [args[1], args[0]]
            return args

    arg_transformer = BinaryPlusGenericDatetimeNumberArgTransformer()
    argument_types = [
        ArgTypeSequence([DataType.GENERICDATETIME, DataType.FLOAT]),
        ArgTypeSequence([DataType.FLOAT, DataType.GENERICDATETIME]),
    ]
    return_type = Fixed(DataType.GENERICDATETIME)


class BinaryMinus(Binary):
    name = "-"
    arg_names = ["value_1", "value_2"]


class BinaryMinusNumbers(BinaryMinus):
    variants = [
        V(D.DUMMY | D.SQLITE, lambda left, right: left - right),
    ]
    argument_types = [
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
    ]
    return_type = FromArgs()


class BinaryMinusInts(BinaryMinus):
    variants = [
        V(D.DUMMY | D.SQLITE, lambda left, right: left - right),
    ]
    argument_types = [
        ArgTypeSequence([DataType.INTEGER, DataType.INTEGER]),
    ]
    return_type = FromArgs()


class BinaryMinusDateInt(BinaryMinus):
    argument_types = [
        ArgTypeSequence([DataType.DATE, DataType.INTEGER]),
    ]
    return_type = Fixed(DataType.DATE)


class BinaryMinusDateFloat(BinaryMinus):
    argument_types = [
        ArgTypeSequence([DataType.DATE, DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.DATE)


class BinaryMinusDatetimeNumber(BinaryMinus):
    argument_types = [
        ArgTypeSequence([DataType.DATETIME, DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.DATETIME)


class BinaryMinusGenericDatetimeNumber(BinaryMinus):
    argument_types = [
        ArgTypeSequence([DataType.GENERICDATETIME, DataType.FLOAT]),
    ]
    return_type = Fixed(DataType.GENERICDATETIME)


class BinaryMinusDates(BinaryMinus):
    variants = [
        V(D.DUMMY, lambda left, right: left - right),
    ]
    argument_types = [
        ArgTypeSequence([DataType.DATE, DataType.DATE]),
    ]
    return_type = Fixed(DataType.INTEGER)


class BinaryMinusDatetimes(BinaryMinus):
    argument_types = [
        ArgTypeSequence([DataType.DATETIME, DataType.DATETIME]),
    ]
    return_type = Fixed(DataType.FLOAT)


class BinaryMinusGenericDatetimes(BinaryMinus):
    argument_types = [
        ArgTypeSequence([DataType.GENERICDATETIME, DataType.GENERICDATETIME]),
    ]
    return_type = Fixed(DataType.FLOAT)


class BinaryComparison(Binary):
    return_type = Fixed(DataType.BOOLEAN)
    return_flags = ContextFlag.IS_CONDITION


class BinaryLike(BinaryComparison):
    name = "like"
    variants = [
        V(D.DUMMY | D.SQLITE, lambda x, y: x.like(y)),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.STRING]),
    ]
    arg_names = ["string_1", "string_2"]


class BinaryNotLike(BinaryComparison):
    name = "notlike"
    scopes = BinaryComparison.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED
    variants = [
        V(D.DUMMY | D.SQLITE, lambda x, y: x.notlike(y)),
    ]
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.STRING]),
    ]
    arg_names = ["string_1", "string_2"]


class BinaryEqNEqOrderedBase(BinaryComparison):
    class BinaryEqNEqOrderedBaseArgTransformer(ArgTransformer):
        def transform_args(
            self, env: TranslationEnvironment, args: list[TranslationCtx], arg_types: list[DataType]
        ) -> list[TranslationCtx]:
            if DataType.NULL in arg_types:
                raise exc.TranslationError("Invalid comparison with NULL (use `<expr> IS [NOT] NULL`)")
            return args

    arg_transformer = BinaryEqNEqOrderedBaseArgTransformer()


COMPARABLE_TYPES: list[ArgTypeMatcher] = [
    ArgTypeSequence([DataType.BOOLEAN, DataType.BOOLEAN]),
    ArgTypeSequence([DataType.STRING, DataType.STRING]),
    ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
    ArgTypeSequence([DataType.DATE, DataType.DATE]),
    ArgTypeSequence([DataType.DATETIME, DataType.DATETIME]),
    ArgTypeSequence([DataType.DATE, DataType.DATETIME]),
    ArgTypeSequence([DataType.DATETIME, DataType.DATE]),
    ArgTypeSequence([DataType.GENERICDATETIME, DataType.GENERICDATETIME]),
    ArgTypeSequence([DataType.DATE, DataType.GENERICDATETIME]),
    ArgTypeSequence([DataType.GENERICDATETIME, DataType.DATE]),
    ArgTypeSequence([DataType.GEOPOINT, DataType.GEOPOINT]),
    ArgTypeSequence([DataType.GEOPOLYGON, DataType.GEOPOLYGON]),
    ArgTypeSequence([DataType.UUID, DataType.UUID]),
    ArgTypeSequence([DataType.ARRAY_INT, DataType.ARRAY_INT]),
    ArgTypeSequence([DataType.ARRAY_FLOAT, DataType.ARRAY_FLOAT]),
    ArgTypeSequence([DataType.ARRAY_STR, DataType.ARRAY_STR]),
]


class BinaryEqNEq(BinaryEqNEqOrderedBase):
    argument_types = COMPARABLE_TYPES


class BinaryEqNEqInternal(BinaryEqNEq):
    argument_types = BinaryEqNEq.argument_types + [
        ArgTypeSequence([DataType.MARKUP, DataType.MARKUP]),
    ]
    # not intended to be used explicitly, only in internal JOINs
    # but JOINs are implemented in the same manner as other formulas, using formula_nodes.Binary,
    # so they won't work without Scope.EXPLICIT_USAGE
    scopes = BinaryEqNEq.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED


class BinaryEqImpl(MultiVariantTranslation):
    variants = [
        V(
            D.DUMMY | D.SQLITE,
            lambda left, right: left == right,
        ),
    ]


class BinaryEqual(BinaryEqImpl, BinaryEqNEq):
    name = "=="
    arg_names = ["value_1", "value_2"]


class BinaryEqualInternal(BinaryEqImpl, BinaryEqNEqInternal):
    name = "_=="


class BinaryEqualDenullified(BinaryEqNEqInternal):
    name = "_dneq"
    scopes = BinaryEqNEqInternal.scopes
    variants = [
        V(D.DUMMY | D.SQLITE, lambda left, right: sa.or_(left == right, sa.and_(left.is_(None), right.is_(None)))),
    ]
    argument_types = BinaryEqNEqInternal.argument_types + [
        ArgTypeSequence([DataType.MARKUP, DataType.MARKUP]),
    ]


class BinaryNEqImpl(MultiVariantTranslation):
    variants = [
        V(D.DUMMY | D.SQLITE, lambda left, right: left != right),
    ]


class BinaryNotEqual(BinaryNEqImpl, BinaryEqNEq):
    name = "!="
    scopes = BinaryEqNEq.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED


class BinaryNotEqualInternal(BinaryNEqImpl, BinaryEqNEqInternal):
    name = "_!="


class BinaryOrderedComparison(BinaryEqNEqOrderedBase):
    argument_types = [
        ArgTypeSequence([DataType.STRING, DataType.STRING]),
        ArgTypeSequence([DataType.FLOAT, DataType.FLOAT]),
        ArgTypeSequence([DataType.DATE, DataType.DATE]),
        ArgTypeSequence([DataType.DATETIME, DataType.DATETIME]),
        ArgTypeSequence([DataType.DATE, DataType.DATETIME]),
        ArgTypeSequence([DataType.DATETIME, DataType.DATE]),
        ArgTypeSequence([DataType.GENERICDATETIME, DataType.GENERICDATETIME]),
        ArgTypeSequence([DataType.DATE, DataType.GENERICDATETIME]),
        ArgTypeSequence([DataType.GENERICDATETIME, DataType.DATE]),
        ArgTypeSequence([DataType.UUID, DataType.UUID]),
    ]


class BinaryLessThan(BinaryOrderedComparison):
    name = "<"
    scopes = BinaryOrderedComparison.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED
    variants = [V(D.DUMMY | D.SQLITE, lambda left, right: left < right)]


class BinaryLessThanOrEqual(BinaryOrderedComparison):
    name = "<="
    scopes = BinaryOrderedComparison.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED
    variants = [
        V(D.DUMMY | D.SQLITE, lambda left, right: left <= right),
    ]


class BinaryGreaterThan(BinaryOrderedComparison):
    name = ">"
    scopes = BinaryOrderedComparison.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED
    variants = [
        V(D.DUMMY | D.SQLITE, lambda left, right: left > right),
    ]
    return_type = Fixed(DataType.BOOLEAN)


class BinaryGreaterThanOrEqual(BinaryOrderedComparison):
    name = ">="
    scopes = BinaryOrderedComparison.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED
    variants = [
        V(D.DUMMY | D.SQLITE, lambda left, right: left >= right),
    ]
    return_type = Fixed(DataType.BOOLEAN)


class BinaryAnd(Binary):  # FIXME: support other types
    name = "and"
    arg_names = ["value_1", "value_2"]
    argument_types = [
        ArgTypeSequence([DataType.BOOLEAN, DataType.BOOLEAN]),
    ]
    argument_flags = ArgFlagSequence([ContextFlag.REQ_CONDITION, ContextFlag.REQ_CONDITION])
    variants = [
        V(D.DUMMY | D.SQLITE, sa.and_),
    ]
    return_type = FromArgs()
    return_flags = ContextFlag.IS_CONDITION


class BinaryOr(Binary):  # FIXME: support other types
    name = "or"
    arg_names = ["value_1", "value_2"]
    argument_types = [
        ArgTypeSequence([DataType.BOOLEAN, DataType.BOOLEAN]),
    ]
    argument_flags = ArgFlagSequence([ContextFlag.REQ_CONDITION, ContextFlag.REQ_CONDITION])
    variants = [
        V(D.DUMMY | D.SQLITE, sa.or_),
    ]
    return_type = FromArgs()
    return_flags = ContextFlag.IS_CONDITION


def _in_fix_null(left, right, stringify_values):
    new_left, new_right = _prepare_in_args(left, right, stringify_values)
    if len(new_right) == len(right):
        return new_left.in_(new_right)

    if len(new_right) > 0:
        return sa.or_(
            new_left.is_(None),
            new_left.in_(new_right),
        )

    return new_left.is_(None)


class BinaryIn(Binary):
    name = "in"
    arg_names = ["item", "list"]
    variants = [
        V(
            D.DUMMY | D.SQLITE,
            functools.partial(_in_fix_null, stringify_values=False),
        ),
    ]
    argument_types = COMPARABLE_TYPES
    return_type = Fixed(DataType.BOOLEAN)
    return_flags = ContextFlag.IS_CONDITION


def _not_in_fix_null(left, right, stringify_values):
    new_left, new_right = _prepare_in_args(left, right, stringify_values)
    if len(new_right) == len(right):
        return sa.or_(
            new_left.notin_(new_right),
            new_left.is_(None),
        )

    if len(new_right) > 0:
        return sa.and_(
            new_left.notin_(new_right),
            new_left.isnot(None),
        )

    return new_left.isnot(None)


def _prepare_in_args(left, right, stringify_values: bool):
    right_nulls_filtered = [x for x in right if not isinstance(x, sa.sql.elements.Null)]
    if not stringify_values:
        return left, right_nulls_filtered

    return left, [sa.func.toString(x) for x in right_nulls_filtered]


class BinaryNotIn(Binary):
    name = "notin"
    scopes = Binary.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED
    arg_names = ["item", "list"]
    variants = [
        V(
            D.DUMMY | D.SQLITE,
            functools.partial(_not_in_fix_null, stringify_values=False),
        ),
    ]
    argument_types = COMPARABLE_TYPES
    return_type = Fixed(DataType.BOOLEAN)
    return_flags = ContextFlag.IS_CONDITION


DEFINITIONS_BINARY = [
    # !=
    BinaryNotEqual,
    # %
    BinaryModInteger,
    BinaryModFloat,
    # *
    BinaryMultNumbers,
    BinaryMultStringConst,
    BinaryMultStringNonConst,
    # +
    BinaryPlusNumbers,
    BinaryPlusStrings,
    BinaryPlusDateInt,
    BinaryPlusArray,
    BinaryPlusDateFloat,
    BinaryPlusDatetimeNumber,
    BinaryPlusGenericDatetimeNumber,
    # -
    BinaryMinusNumbers,
    BinaryMinusDateInt,
    BinaryMinusDateFloat,
    BinaryMinusDatetimeNumber,
    BinaryMinusGenericDatetimeNumber,
    BinaryMinusDates,
    BinaryMinusDatetimes,
    BinaryMinusGenericDatetimes,
    # /
    BinaryDivInt,
    BinaryDivFloat,
    # <
    BinaryLessThan,
    # <=
    BinaryLessThanOrEqual,
    # ==
    BinaryEqual,
    # >
    BinaryGreaterThan,
    # >=
    BinaryGreaterThanOrEqual,
    # ^
    BinaryPower,
    # _!=
    BinaryNotEqualInternal,
    # _==
    BinaryEqualInternal,
    # _dneq
    BinaryEqualDenullified,
    # and
    BinaryAnd,
    # in
    BinaryIn,
    # like
    BinaryLike,
    # notin
    BinaryNotIn,
    # notlike
    BinaryNotLike,
    # or
    BinaryOr,
]
