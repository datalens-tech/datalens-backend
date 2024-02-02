import sqlalchemy as sa

from dl_formula.core.datatype import DataType
from dl_formula.definitions.args import ArgTypeSequence
from dl_formula.definitions.base import (
    Function,
    SingleVariantTranslationBase,
    TranslationVariant,
)
import dl_formula.definitions.functions_type as base
from dl_formula.definitions.scope import Scope
from dl_formula.definitions.type_strategy import Fixed

from dl_connector_snowflake.formula.constants import SnowFlakeDialect as D


V = TranslationVariant.make


# todo: review datetime * timezone * snowflake logic
#   copied from postgresql connector


class FuncDatetime2SF(SingleVariantTranslationBase, base.FuncTypeGenericDatetime2Impl):
    dialects = D.SNOWFLAKE
    argument_types = [
        ArgTypeSequence(
            [
                {DataType.DATETIME, DataType.GENERICDATETIME, DataType.INTEGER, DataType.FLOAT, DataType.STRING},
                DataType.CONST_STRING,
            ]
        ),
    ]
    name = "datetime"

    @classmethod
    def _translate_main(cls, value_ctx, tz_ctx):  # type: ignore  # 2024-01-30 # TODO: Function is missing a type annotation  [no-untyped-def]
        """
        Equivalent to `dt at time zone tz`.
        Its semantics:

        1. For `timestamp` (naive) values, the value is interpreted
          at the specified timezone (with ambiguity),
          returning `timestamptz` (utc). Example:
          `select timezone('Europe/Moscow', '2010-10-31T02:00:00'::timestamp);`
          -> `2010-10-30 23:00:00+00`.
        2. For `timestamptz` (aware) values, the value is shifted to local time,
          returning `timestamp` (naive).
          Example:
          `select timezone('Europe/Moscow', timezone('Europe/Moscow', '2010-10-31T02:00:00'::timestamp));`
          -> `2010-10-31 02:00:00` (back to an ambiguous value).
        3. String / integer / float values are not accepted (however, some pg
          clients might autocast strings).
        4. `date` argument works, but it doesn't seem useful. Example:
          `select timezone('Europe/Moscow', '2010-10-31'::date);`
          -> `2010-10-31 03:00:00`

        This implementation provides a way to make datetime from string / integer / float values but it fully relies
        on database when it comes to transforming between naive and aware values.

        If input value is naive timestamp or string (will be casted to naive as well),
        the result will be tz-aware value (see 1. above)
        but in case of input being tz-aware timestamp or integer / float (casted to tz-aware utc value)
        the result will be naive (see 2. above)
        """
        value = value_ctx.expression
        value_type = value_ctx.data_type
        tz = tz_ctx.expression

        if value_type in (
            DataType.DATETIME,
            DataType.CONST_DATETIME,
            DataType.GENERICDATETIME,
            DataType.CONST_GENERICDATETIME,
        ):
            pass  # value = value
        elif value_type in (DataType.INTEGER, DataType.CONST_INTEGER, DataType.FLOAT, DataType.CONST_FLOAT):
            value = sa.func.to_timestamp(value)
        elif value_type in (DataType.STRING, DataType.CONST_STRING):
            value = sa.cast(value, sa.DateTime())
        else:
            raise Exception("Unexpected value type, likely a translation definition error", value_type)

        return sa.func.convert_timezone(tz, value)


class FuncGenericDatetime2SF(FuncDatetime2SF):
    name = "genericdatetime"
    scopes = Function.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED


class FuncBoolFromNullSF(base.FuncBoolFromNull):
    variants = [
        V(D.SNOWFLAKE, lambda _: sa.null()),
    ]
    return_type = Fixed(DataType.NULL)


class FuncFloatFromNullSF(base.FuncFloat):
    variants = [
        V(D.SNOWFLAKE, lambda _: sa.null()),
    ]
    argument_types = [
        ArgTypeSequence([DataType.NULL]),
    ]


DEFINITIONS_TYPE = [
    # bool
    FuncBoolFromNullSF(),
    base.FuncBoolFromNumber.for_dialect(D.SNOWFLAKE),
    base.FuncBoolFromBool.for_dialect(D.SNOWFLAKE),
    base.FuncBoolFromStrGeo.for_dialect(D.SNOWFLAKE),
    base.FuncBoolFromDateDatetime.for_dialect(D.SNOWFLAKE),
    # date
    base.FuncDate1FromNull.for_dialect(D.SNOWFLAKE),
    base.FuncDate1FromDatetime(
        variants=[
            V(D.SNOWFLAKE, lambda expr: sa.func.TO_DATE(expr)),
        ]
    ),
    base.FuncDate1FromString(
        variants=[
            V(D.SNOWFLAKE, lambda expr: sa.func.TO_DATE(expr, "YYYY-MM-DD")),
        ]
    ),
    base.FuncDate1FromNumber(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda expr: sa.func.TO_DATE(sa.func.TO_CHAR(expr)),
            ),
        ]
    ),
    # datetime
    base.FuncDatetime1FromNull.for_dialect(D.SNOWFLAKE),
    base.FuncDatetime1FromDatetime.for_dialect(D.SNOWFLAKE),
    base.FuncDatetime1FromDate.for_dialect(D.SNOWFLAKE),
    base.FuncDatetime1FromNumber(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda expr: sa.func.TO_TIMESTAMP(expr),
            )
        ]
    ),
    base.FuncDatetime1FromString(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda value: sa.func.to_timestamp(value),
            )
        ]
    ),
    FuncDatetime2SF.for_dialect(D.SNOWFLAKE),
    # datetimetz
    base.FuncDatetimeTZConst.for_dialect(D.SNOWFLAKE),
    # float
    FuncFloatFromNullSF(),
    base.FuncFloatNumber(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.TO_DOUBLE(value)),
        ]
    ),
    base.FuncFloatString(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.TO_DOUBLE(value)),
        ]
    ),
    base.FuncFloatFromBool(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda value: sa.case(
                    whens=[
                        (value.is_(None), sa.null()),
                        (value == True, 1.0),  # noqa: E712
                        (value == False, 0.0),  # noqa: E712
                    ],
                ),
            )
        ]
    ),
    base.FuncFloatFromDate(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.TO_DOUBLE(sa.func.DATE_PART("epoch_second", value))),
        ]
    ),
    base.FuncFloatFromDatetime(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.TO_DOUBLE(sa.func.DATE_PART("epoch_second", value))),
        ]
    ),
    base.FuncFloatFromGenericDatetime(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.TO_DOUBLE(sa.func.DATE_PART("epoch_second", value))),
        ]
    ),
    # genericdatetime
    base.FuncGenericDatetime1FromNull.for_dialect(D.SNOWFLAKE),
    base.FuncGenericDatetime1FromDatetime.for_dialect(D.SNOWFLAKE),
    base.FuncGenericDatetime1FromDate.for_dialect(D.SNOWFLAKE),
    base.FuncGenericDatetime1FromNumber(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda expr: sa.func.TO_TIMESTAMP(expr),
            )
        ]
    ),
    base.FuncGenericDatetime1FromString.for_dialect(D.SNOWFLAKE),
    FuncGenericDatetime2SF.for_dialect(D.SNOWFLAKE),
    # geopoint
    base.FuncGeopointFromStr.for_dialect(D.SNOWFLAKE),  # todo:
    base.FuncGeopointFromCoords.for_dialect(D.SNOWFLAKE),  # todo:
    # geopolygon
    base.FuncGeopolygon.for_dialect(D.SNOWFLAKE),  # todo:
    # int
    base.FuncIntFromNull(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.TO_NUMBER(value)),
        ]
    ),
    base.FuncIntFromInt(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.TO_NUMBER(value)),
        ]
    ),
    base.FuncIntFromFloat(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.TO_NUMBER(sa.func.FLOOR(value))),
        ]
    ),
    base.FuncIntFromBool(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.TO_NUMBER(value)),
        ]
    ),
    base.FuncIntFromStr(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.TO_NUMBER(value)),
        ]
    ),
    base.FuncIntFromDate(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.DATE_PART("epoch_second", value)),
        ]
    ),
    base.FuncIntFromDatetime(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.DATE_PART("epoch_second", value)),
        ]
    ),
    base.FuncIntFromGenericDatetime(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.DATE_PART("epoch_second", value)),
        ]
    ),
    base.FuncIntFromDatetimeTZ(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.func.DATE_PART("epoch_second", value)),
        ]
    ),
    # str
    base.FuncStrFromNull(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.cast(sa.null(), sa.String())),
        ]
    ),
    base.FuncStrFromUnsupported(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.cast(value, sa.String())),
        ]
    ),
    base.FuncStrFromInteger(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.cast(value, sa.String())),
        ]
    ),
    base.FuncStrFromFloat(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.cast(value, sa.String())),
        ]
    ),
    base.FuncStrFromBool(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.case(whens=[(value.is_(None), sa.null()), (value, "True")], else_="False")),
        ]
    ),
    base.FuncStrFromStrGeo.for_dialect(D.SNOWFLAKE),
    base.FuncStrFromDate(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda value: sa.func.TO_CHAR(value),
            ),
        ]
    ),
    base.FuncStrFromDatetime(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda value: sa.func.LEFT(sa.func.TO_CHAR(sa.func.to_timestamp_ltz(value)), 19),
            ),
        ]
    ),
    base.FuncStrFromDatetimeTZ(
        variants=[
            V(
                D.SNOWFLAKE,
                lambda value: sa.func.LEFT(sa.func.TO_CHAR(sa.func.to_timestamp_ltz(value)), 19),
            ),
        ]
    ),
    base.FuncStrFromString.for_dialect(D.SNOWFLAKE),
    base.FuncStrFromUUID(
        variants=[
            V(D.SNOWFLAKE, lambda value: sa.cast(value, sa.String())),
        ]
    ),
    # base.FuncStrFromArray,  # FIXME
]
