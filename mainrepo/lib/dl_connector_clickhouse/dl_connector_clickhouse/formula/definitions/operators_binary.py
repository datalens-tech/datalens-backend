import functools
from typing import Any

import sqlalchemy as sa
from sqlalchemy.sql.elements import ClauseElement

from dl_connector_clickhouse.formula.constants import ClickHouseDialect as D
from dl_formula.core.datatype import DataType
from dl_formula.core.dialect import DialectCombo
from dl_formula.definitions.args import ArgTypeSequence
from dl_formula.definitions.base import (
    TranslationVariant,
    TranslationVariantWrapped,
)
from dl_formula.definitions.common import raw_sql
from dl_formula.definitions.common_datetime import DAY_SEC
from dl_formula.definitions.literals import (
    Literal,
    literal,
)
import dl_formula.definitions.operators_binary as base
from dl_formula.translation.context import TranslationCtx
from dl_formula.translation.env import TranslationEnvironment


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


class CHBinaryEqNEqOrderedBaseArgTransformer(
    base.BinaryEqNEqOrderedBase.BinaryEqNEqOrderedBaseArgTransformer,
):
    @staticmethod
    def _to_datetime(value: TranslationCtx) -> TranslationCtx:
        value = value.copy()
        value.set_expression(sa.func.toDateTime(value.expression, "UTC"))
        value.set_type(DataType.DATETIME)
        return value

    @staticmethod
    def _to_genericdatetime(value: TranslationCtx) -> TranslationCtx:
        value = value.copy()
        value.set_expression(sa.func.toDateTime(value.expression))
        value.set_type(DataType.GENERICDATETIME)
        return value

    def transform_args(
        self, env: TranslationEnvironment, args: list[TranslationCtx], arg_types: list[DataType]
    ) -> list[TranslationCtx]:
        args = super().transform_args(env=env, args=args, arg_types=arg_types)

        if env.dialect & D.CLICKHOUSE:  # FIXME: Connectorize
            # if `args` is a mix of DATE and DATETIME/GENERICDATETIME, then DATE must be cast to DATETIME/GENERICDATETIME
            # TODO: DATETIMETZ
            if arg_types[0] in {DataType.DATE, DataType.CONST_DATE} and arg_types[1] in {
                DataType.DATETIME,
                DataType.CONST_DATETIME,
            }:
                args = [self._to_datetime(args[0]), args[1]]
            elif arg_types[0] in {DataType.DATE, DataType.CONST_DATE} and arg_types[1] in {
                DataType.GENERICDATETIME,
                DataType.CONST_GENERICDATETIME,
            }:
                args = [self._to_genericdatetime(args[0]), args[1]]
            elif arg_types[0] in {DataType.DATETIME, DataType.CONST_DATETIME} and arg_types[1] in {
                DataType.DATE,
                DataType.CONST_DATE,
            }:
                args = [args[0], self._to_datetime(args[1])]
            elif arg_types[0] in {DataType.GENERICDATETIME, DataType.CONST_GENERICDATETIME} and arg_types[1] in {
                DataType.DATE,
                DataType.CONST_DATE,
            }:
                args = [args[0], self._to_genericdatetime(args[1])]

        return args


def _get_null_substitute_value(data_type: DataType, dialect: DialectCombo) -> tuple[Literal, bool]:
    def lit(v: Any) -> Literal:
        return literal(v, d=dialect)

    stringify_value = False

    null_value: Literal
    if data_type in {
        DataType.STRING,
        DataType.CONST_STRING,
        DataType.GEOPOINT,
        DataType.CONST_GEOPOINT,
        DataType.GEOPOLYGON,
        DataType.CONST_GEOPOLYGON,
        DataType.MARKUP,
        DataType.CONST_MARKUP,
    }:
        null_value = lit("-")
    elif data_type in {
        DataType.DATE,
        DataType.CONST_DATE,
        DataType.DATETIME,
        DataType.CONST_DATETIME,
        DataType.GENERICDATETIME,
        DataType.CONST_GENERICDATETIME,
    }:
        null_value = lit("-")
        # TODO: Revert to using date values once dialects prior to 22.10 are no longer supported
        stringify_value = True  # Convert original value to string
    elif data_type in {
        DataType.INTEGER,
        DataType.CONST_INTEGER,
        DataType.FLOAT,
        DataType.CONST_FLOAT,
        DataType.BOOLEAN,
        DataType.CONST_BOOLEAN,
    }:
        null_value = lit(0)
    elif data_type in {DataType.UUID, DataType.CONST_UUID}:
        null_value = sa.func.toUUID(lit("00000000-0000-0000-0000-000000000000"))
    elif data_type in {
        DataType.ARRAY_INT,
        DataType.CONST_ARRAY_INT,
        DataType.ARRAY_FLOAT,
        DataType.CONST_ARRAY_FLOAT,
    }:
        null_value = lit([0])
    elif data_type in {
        DataType.ARRAY_STR,
        DataType.CONST_ARRAY_STR,
        DataType.TREE_STR,
        DataType.CONST_TREE_STR,
    }:
        null_value = lit(["__null_value__"])
    else:
        raise ValueError

    return null_value, stringify_value


def _denullified_eq(
    left_ctx: TranslationCtx,
    right_ctx: TranslationCtx,
    translation_env: TranslationEnvironment,
) -> ClauseElement:
    data_type = left_ctx.data_type
    assert data_type is not None

    right_type = right_ctx.data_type
    assert right_type is not None
    assert right_type.non_const_type == data_type.non_const_type

    if data_type == DataType.UNSUPPORTED:
        # We don't know the real type,
        # so the best we can do is build a regular equality expression
        return left_ctx.expression == right_ctx.expression  # type: ignore

    dialect = translation_env.dialect
    null_value, stringify_value = _get_null_substitute_value(data_type, dialect=dialect)

    left = left_ctx.expression
    right = right_ctx.expression
    if stringify_value:
        left = sa.func.toString(left)
        right = sa.func.toString(right)

    expr = sa.and_(
        sa.func.ifNull(left, null_value) == sa.func.ifNull(right, null_value),
        sa.func.isNull(left) == sa.func.isNull(right),
    )
    return expr  # type: ignore


class BinaryEqualDenullified(base.BinaryEqualDenullified):
    variants = [
        VW(D.CLICKHOUSE, lambda left, right, _env: _denullified_eq(left, right, _env)),
    ]


class BinaryInDate(base.BinaryIn):
    argument_types = [
        ArgTypeSequence([DataType.DATE, DataType.DATE]),
    ]
    variants = [
        V(
            D.CLICKHOUSE_22_10,
            functools.partial(base._in_fix_null, stringify_values=True),  # noqa
        ),
    ]


class BinaryNotInDate(base.BinaryNotIn):
    argument_types = [
        ArgTypeSequence([DataType.DATE, DataType.DATE]),
    ]
    variants = [
        V(
            D.CLICKHOUSE_22_10,
            functools.partial(base._not_in_fix_null, stringify_values=True),
        ),
    ]


DEFINITIONS_BINARY = [
    # !=
    base.BinaryNotEqual.for_dialect(D.CLICKHOUSE, arg_transformer=CHBinaryEqNEqOrderedBaseArgTransformer()),
    # %
    base.BinaryModInteger.for_dialect(D.CLICKHOUSE),
    base.BinaryModFloat(
        variants=[
            V(D.CLICKHOUSE, lambda left, right: left - sa.func.floor(left / right) * right),
        ]
    ),
    # *
    base.BinaryMultNumbers.for_dialect(D.CLICKHOUSE),
    base.BinaryMultStringConst.for_dialect(D.CLICKHOUSE),
    base.BinaryMultStringNonConst(
        variants=[
            V(
                D.CLICKHOUSE,
                lambda text, size: sa.func.arrayStringConcat(
                    sa.func.arrayResize(sa.func.emptyArrayString(), size, text)
                ),
            ),
        ]
    ),
    # +
    base.BinaryPlusNumbers.for_dialect(D.CLICKHOUSE),
    base.BinaryPlusStrings.for_dialect(D.CLICKHOUSE),
    base.BinaryPlusDateInt(
        variants=[
            V(D.CLICKHOUSE, lambda date, days: sa.func.date_add(raw_sql("day"), days, date)),
        ]
    ),
    base.BinaryPlusArray(
        variants=[
            V(D.CLICKHOUSE, sa.func.arrayConcat),
        ]
    ),
    base.BinaryPlusDateFloat(
        variants=[
            V(D.CLICKHOUSE, lambda date, days: sa.func.date_add(raw_sql("day"), base.as_bigint(days), date)),
        ]
    ),
    base.BinaryPlusDatetimeNumber(
        variants=[
            V(D.CLICKHOUSE, lambda dt, days: sa.func.toDateTime(dt + base.as_bigint(days * DAY_SEC), "UTC")),
        ]
    ),
    base.BinaryPlusGenericDatetimeNumber(
        variants=[
            V(D.CLICKHOUSE, lambda dt, days: sa.func.date_add(raw_sql("second"), days * DAY_SEC, dt)),
        ]
    ),
    # -
    base.BinaryMinusNumbers.for_dialect(D.CLICKHOUSE),
    base.BinaryMinusDateInt(
        variants=[
            V(D.CLICKHOUSE, lambda date, days: sa.func.date_add(raw_sql("day"), -days, date)),
        ]
    ),
    base.BinaryMinusDateFloat(
        variants=[
            V(
                D.CLICKHOUSE,
                lambda date, days: sa.func.date_add(raw_sql("day"), base.as_bigint(-sa.func.ceil(days)), date),
            ),
        ]
    ),
    base.BinaryMinusDatetimeNumber(
        variants=[
            V(D.CLICKHOUSE, lambda dt, days: sa.func.toDateTime(dt - base.as_bigint(days * DAY_SEC), "UTC")),
        ]
    ),
    base.BinaryMinusGenericDatetimeNumber(
        variants=[
            V(D.CLICKHOUSE, lambda dt, days: sa.func.date_sub(raw_sql("second"), days * DAY_SEC, dt)),
        ]
    ),
    base.BinaryMinusDates(
        variants=[
            V(D.CLICKHOUSE, lambda left, right: sa.func.DATEDIFF("day", right, left)),
        ]
    ),
    base.BinaryMinusDatetimes(
        variants=[
            V(D.CLICKHOUSE, lambda left, right: (sa.func.toUInt32(left) - sa.func.toUInt32(right)) / DAY_SEC),
        ]
    ),
    base.BinaryMinusGenericDatetimes(
        variants=[
            V(D.CLICKHOUSE, lambda left, right: sa.func.DATEDIFF("second", right, left) / DAY_SEC),
        ]
    ),
    # /
    base.BinaryDivInt.for_dialect(D.CLICKHOUSE),
    base.BinaryDivFloat.for_dialect(D.CLICKHOUSE),
    # <
    base.BinaryLessThan.for_dialect(D.CLICKHOUSE, arg_transformer=CHBinaryEqNEqOrderedBaseArgTransformer()),
    # <=
    base.BinaryLessThanOrEqual.for_dialect(D.CLICKHOUSE, arg_transformer=CHBinaryEqNEqOrderedBaseArgTransformer()),
    # ==
    base.BinaryEqual.for_dialect(D.CLICKHOUSE, arg_transformer=CHBinaryEqNEqOrderedBaseArgTransformer()),
    # >
    base.BinaryGreaterThan.for_dialect(D.CLICKHOUSE, arg_transformer=CHBinaryEqNEqOrderedBaseArgTransformer()),
    # >=
    base.BinaryGreaterThanOrEqual.for_dialect(D.CLICKHOUSE, arg_transformer=CHBinaryEqNEqOrderedBaseArgTransformer()),
    # ^
    base.BinaryPower.for_dialect(D.CLICKHOUSE),
    # _!=
    base.BinaryNotEqualInternal.for_dialect(D.CLICKHOUSE, arg_transformer=CHBinaryEqNEqOrderedBaseArgTransformer()),
    # _==
    base.BinaryEqualInternal.for_dialect(D.CLICKHOUSE, arg_transformer=CHBinaryEqNEqOrderedBaseArgTransformer()),
    # _dneq
    BinaryEqualDenullified(arg_transformer=CHBinaryEqNEqOrderedBaseArgTransformer()),
    # and
    base.BinaryAnd.for_dialect(D.CLICKHOUSE),
    # in
    BinaryInDate(),
    base.BinaryIn.for_dialect(D.CLICKHOUSE),
    # like
    base.BinaryLike.for_dialect(D.CLICKHOUSE),
    # notin
    BinaryNotInDate(),
    base.BinaryNotIn.for_dialect(D.CLICKHOUSE),
    # notlike
    base.BinaryNotLike.for_dialect(D.CLICKHOUSE),
    # or
    base.BinaryOr.for_dialect(D.CLICKHOUSE),
]
