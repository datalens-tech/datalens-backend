import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_postgresql

from dl_formula.core.datatype import DataType
from dl_formula.definitions.args import ArgTypeSequence
from dl_formula.definitions.base import (
    Function,
    SingleVariantTranslationBase,
    TranslationVariant,
    TranslationVariantWrapped,
)
from dl_formula.definitions.common_datetime import ensure_naive_first_arg
import dl_formula.definitions.functions_type as base
from dl_formula.definitions.scope import Scope
from dl_formula.shortcuts import n

from dl_connector_postgresql.formula.constants import PostgreSQLDialect as D
from dl_connector_postgresql.formula.definitions.common import PG_INT_64_TO_CHAR_FMT


V = TranslationVariant.make
VW = TranslationVariantWrapped.make


class FuncTypeGenericDatetime2PGImpl(SingleVariantTranslationBase, base.FuncTypeGenericDatetime2Impl):
    dialects = D.POSTGRESQL
    argument_types = [
        ArgTypeSequence(
            [
                {DataType.DATETIME, DataType.GENERICDATETIME, DataType.INTEGER, DataType.FLOAT, DataType.STRING},
                DataType.CONST_STRING,
            ]
        ),
    ]

    @classmethod
    def _translate_main(cls, value_ctx, tz_ctx):  # type: ignore
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

        return sa.func.timezone(tz, value)


class FuncDatetimeTZPG(SingleVariantTranslationBase, base.FuncDatetimeTZ):
    dialects = D.POSTGRESQL
    argument_types = [
        ArgTypeSequence(
            [
                {
                    DataType.DATETIME,
                    DataType.GENERICDATETIME,
                    DataType.DATETIMETZ,
                    # Not even tried: `DataType.DATE`.
                    DataType.INTEGER,
                    DataType.FLOAT,
                    DataType.STRING,
                },
                DataType.CONST_STRING,
            ]
        ),
    ]

    @classmethod
    def _translate_main(cls, value_ctx, tz_ctx):  # type: ignore
        value = value_ctx.expression
        value_type = value_ctx.data_type
        tz = tz_ctx.expression

        if value_type == DataType.DATETIMETZ:
            # Only change the data_type_params, leave the value
            return value

        if value_type == DataType.GENERICDATETIME:
            return value

        if value_type == DataType.DATETIME:
            value = sa.cast(value, sa.TIMESTAMP())  # ensure naive dt
            return sa.func.timezone(tz, value)  # make `timestamptz`

        if value_type in (DataType.INTEGER, DataType.FLOAT):
            value = sa.func.to_timestamp(value)  # -> `timestamptz`
            # The timezone goes into the data_type_params.
            return value

        elif value_type == DataType.STRING:
            # Casting to `timestamptz` ensures tzoffset in ISO strings isn't ignored.
            value = sa.cast(value, sa.TIMESTAMP(timezone=True))
            # Then making the naive datetime of the value minus the offset (if it had one)
            value = sa.cast(value, sa.TIMESTAMP(timezone=False))
            # Then interpreting the naive datetime in the specified timezone
            return sa.func.timezone(tz, value)

        raise Exception("Unexpected value type, likely a translation definition error", value_type)


class FuncDatetime2PG(FuncTypeGenericDatetime2PGImpl):
    name = "datetime"


class FuncGenericDatetime2PG(FuncTypeGenericDatetime2PGImpl):
    name = "genericdatetime"
    scopes = Function.scopes & ~Scope.SUGGESTED & ~Scope.DOCUMENTED


class FuncDatetimeTZToNaivePG(base.FuncDatetimeTZToNaive):
    dialects = D.POSTGRESQL | D.COMPENG

    @classmethod
    def _translate_main(cls, value_ctx):  # type: ignore
        expr = value_ctx.expression
        tz = value_ctx.data_type_params.timezone
        assert tz

        # Ensure the value is tz-aware in the postgresql:
        expr = sa.cast(expr, sa.TIMESTAMP(timezone=True))
        # Convert to postgresql's tz-naive value:
        expr = sa.func.timezone(tz, expr)
        return expr


class FuncDbCastPostgreSQLBase(base.FuncDbCastBase):
    WHITELISTS = {
        pg_dialect: {
            DataType.INTEGER: [
                base.WhitelistTypeSpec(name="smallint", sa_type=sa_postgresql.SMALLINT),
                base.WhitelistTypeSpec(name="integer", sa_type=sa_postgresql.INTEGER),
                base.WhitelistTypeSpec(name="bigint", sa_type=sa_postgresql.BIGINT),
            ],
            DataType.FLOAT: [
                base.WhitelistTypeSpec(name="double precision", sa_type=sa_postgresql.DOUBLE_PRECISION),
                base.WhitelistTypeSpec(name="real", sa_type=sa_postgresql.REAL),
                base.WhitelistTypeSpec(
                    name="numeric", sa_type=sa_postgresql.NUMERIC, arg_types=base.DECIMAL_CAST_ARG_T
                ),
            ],
            DataType.STRING: [
                base.WhitelistTypeSpec(name="text", sa_type=sa_postgresql.TEXT),
                base.WhitelistTypeSpec(name="character", sa_type=sa_postgresql.CHAR, arg_types=base.CHAR_CAST_ARG_T),
                base.WhitelistTypeSpec(
                    name="character varying", sa_type=sa_postgresql.VARCHAR, arg_types=base.CHAR_CAST_ARG_T
                ),
                # Here we'll make an exception and allow the usage of type aliases
                # just because in this case they are probably much more widely used
                # then the proper type names:
                base.WhitelistTypeSpec(name="char", sa_type=sa_postgresql.CHAR, arg_types=base.CHAR_CAST_ARG_T),
                base.WhitelistTypeSpec(name="varchar", sa_type=sa_postgresql.VARCHAR, arg_types=base.CHAR_CAST_ARG_T),
            ],
            DataType.ARRAY_STR: [
                base.WhitelistTypeSpec(name="text[]", sa_type=sa_postgresql.ARRAY, nested_sa_type=sa_postgresql.TEXT),
                base.WhitelistTypeSpec(
                    name="character varying[]", sa_type=sa_postgresql.ARRAY, nested_sa_type=sa_postgresql.VARCHAR
                ),
                base.WhitelistTypeSpec(
                    name="varchar[]", sa_type=sa_postgresql.ARRAY, nested_sa_type=sa_postgresql.VARCHAR
                ),
            ],
            DataType.ARRAY_INT: [
                base.WhitelistTypeSpec(
                    name="smallint[]", sa_type=sa_postgresql.ARRAY, nested_sa_type=sa_postgresql.SMALLINT
                ),
                base.WhitelistTypeSpec(
                    name="integer[]", sa_type=sa_postgresql.ARRAY, nested_sa_type=sa_postgresql.INTEGER
                ),
                base.WhitelistTypeSpec(
                    name="bigint[]", sa_type=sa_postgresql.ARRAY, nested_sa_type=sa_postgresql.BIGINT
                ),
            ],
            DataType.ARRAY_FLOAT: [
                base.WhitelistTypeSpec(
                    name="double precision[]",
                    sa_type=sa_postgresql.ARRAY,
                    nested_sa_type=sa_postgresql.DOUBLE_PRECISION,
                ),
                base.WhitelistTypeSpec(name="real[]", sa_type=sa_postgresql.ARRAY, nested_sa_type=sa_postgresql.REAL),
                base.WhitelistTypeSpec(
                    name="numeric[]",
                    sa_type=sa_postgresql.ARRAY,
                    nested_sa_type=sa_postgresql.NUMERIC,
                    arg_types=base.DECIMAL_CAST_ARG_T,
                ),
            ],
        }
        for pg_dialect in (D.COMPENG, D.NON_COMPENG_POSTGRESQL)
    }


class FuncDbCastPostgreSQL2(FuncDbCastPostgreSQLBase, base.FuncDbCast2):
    pass


class FuncDbCastPostgreSQL3(FuncDbCastPostgreSQLBase, base.FuncDbCast3):
    pass


class FuncDbCastPostgreSQL4(FuncDbCastPostgreSQLBase, base.FuncDbCast4):
    pass


DEFINITIONS_TYPE = [
    # bool
    base.FuncBoolFromNull.for_dialect(D.POSTGRESQL),
    base.FuncBoolFromNumber.for_dialect(D.POSTGRESQL),
    base.FuncBoolFromBool.for_dialect(D.POSTGRESQL),
    base.FuncBoolFromStrGeo.for_dialect(D.POSTGRESQL),
    base.FuncBoolFromDateDatetime.for_dialect(D.POSTGRESQL),
    # date
    base.FuncDate1FromNull.for_dialect(D.POSTGRESQL),
    base.FuncDate1FromDatetime.for_dialect(D.POSTGRESQL),
    base.FuncDate1FromDatetimeTZ(
        variants=[
            VW(D.POSTGRESQL, ensure_naive_first_arg(n.func.DATE)),
        ]
    ),
    base.FuncDate1FromString.for_dialect(D.POSTGRESQL),
    base.FuncDate1FromNumber(
        variants=[
            V(D.POSTGRESQL, lambda expr: sa.cast(sa.func.TO_TIMESTAMP(expr), sa.Date())),
        ]
    ),
    # datetime
    base.FuncDatetime1FromNull.for_dialect(D.POSTGRESQL),
    base.FuncDatetime1FromDatetime.for_dialect(D.POSTGRESQL),
    base.FuncDatetime1FromDate.for_dialect(D.POSTGRESQL),
    base.FuncDatetime1FromNumber(
        variants=[
            V(D.POSTGRESQL, lambda expr: sa.func.TO_TIMESTAMP(expr)),
        ]
    ),
    base.FuncDatetime1FromString.for_dialect(D.POSTGRESQL),
    FuncDatetime2PG(),
    # datetimetz
    base.FuncDatetimeTZConst.for_dialect(D.POSTGRESQL),
    FuncDatetimeTZPG(),
    # datetimetz_to_naive
    FuncDatetimeTZToNaivePG(),
    # db_cast
    FuncDbCastPostgreSQL2(),
    FuncDbCastPostgreSQL3(),
    FuncDbCastPostgreSQL4(),
    # float
    base.FuncFloatNumber(
        variants=[
            V(D.POSTGRESQL, lambda value: sa.cast(value, sa_postgresql.DOUBLE_PRECISION)),
        ]
    ),
    base.FuncFloatString(
        variants=[
            V(D.POSTGRESQL, lambda value: sa.cast(value, sa_postgresql.DOUBLE_PRECISION)),
        ]
    ),
    base.FuncFloatFromBool(
        variants=[
            V(D.POSTGRESQL, lambda value: sa.cast(sa.cast(value, sa.INTEGER), sa_postgresql.DOUBLE_PRECISION)),
        ]
    ),
    base.FuncFloatFromDate(
        variants=[
            V(D.POSTGRESQL, lambda value: sa.cast(sa.func.extract("epoch", value), sa_postgresql.DOUBLE_PRECISION)),
        ]
    ),
    base.FuncFloatFromDatetime(
        variants=[
            V(D.POSTGRESQL, lambda value: sa.cast(sa.func.extract("epoch", value), sa_postgresql.DOUBLE_PRECISION)),
        ]
    ),
    base.FuncFloatFromGenericDatetime(
        variants=[
            V(D.POSTGRESQL, lambda value: sa.cast(sa.func.extract("epoch", value), sa_postgresql.DOUBLE_PRECISION)),
        ]
    ),
    # genericdatetime
    base.FuncGenericDatetime1FromNull.for_dialect(D.POSTGRESQL),
    base.FuncGenericDatetime1FromDatetime.for_dialect(D.POSTGRESQL),
    base.FuncGenericDatetime1FromDate.for_dialect(D.POSTGRESQL),
    base.FuncGenericDatetime1FromNumber(
        variants=[
            V(D.POSTGRESQL, lambda expr: sa.func.TO_TIMESTAMP(expr)),
        ]
    ),
    base.FuncGenericDatetime1FromString.for_dialect(D.POSTGRESQL),
    FuncGenericDatetime2PG(),
    # geopoint
    base.FuncGeopointFromStr.for_dialect(D.POSTGRESQL),
    base.FuncGeopointFromCoords.for_dialect(D.POSTGRESQL),
    # geopolygon
    base.FuncGeopolygon.for_dialect(D.POSTGRESQL),
    # int
    base.FuncIntFromNull(
        variants=[
            V(D.POSTGRESQL, lambda _: sa.cast(sa.null(), sa.BIGINT())),
        ]
    ),
    base.FuncIntFromInt.for_dialect(D.POSTGRESQL),
    base.FuncIntFromFloat(
        variants=[
            V(D.POSTGRESQL, lambda value: sa.cast(sa.func.FLOOR(value), sa.BIGINT)),
        ]
    ),
    base.FuncIntFromBool(
        variants=[
            # Direct cast bool->bigint is not supported; yet, casting to bigint for type consistency.
            V(D.POSTGRESQL, lambda value: sa.cast(sa.cast(value, sa.INTEGER), sa.BIGINT)),
        ]
    ),
    base.FuncIntFromStr(
        variants=[
            V(D.POSTGRESQL, lambda value: sa.cast(value, sa.BIGINT)),
        ]
    ),
    base.FuncIntFromDate(
        variants=[
            V(D.POSTGRESQL, lambda value: sa.cast(sa.func.extract("epoch", value), sa.BIGINT)),
        ]
    ),
    base.FuncIntFromDatetime(
        variants=[
            V(D.POSTGRESQL, lambda value: sa.cast(sa.func.extract("epoch", value), sa.BIGINT)),
        ]
    ),
    base.FuncIntFromGenericDatetime(
        variants=[
            V(D.POSTGRESQL, lambda value: sa.cast(sa.func.extract("epoch", value), sa.BIGINT)),
        ]
    ),
    base.FuncIntFromDatetimeTZ(
        variants=[
            V(D.POSTGRESQL, lambda value: sa.cast(sa.func.extract("epoch", value), sa.BIGINT)),
        ]
    ),
    # str
    base.FuncStrFromNull(
        variants=[
            V(D.POSTGRESQL, lambda value: sa.cast(sa.null(), sa.String())),
        ]
    ),
    base.FuncStrFromUnsupported(
        variants=[
            V(D.POSTGRESQL, lambda value: sa.cast(value, sa.TEXT)),  # supports more than `to_char`
        ]
    ),
    base.FuncStrFromInteger(
        variants=[
            V(D.POSTGRESQL, lambda value: sa.func.TO_CHAR(value, PG_INT_64_TO_CHAR_FMT)),
        ]
    ),
    base.FuncStrFromFloat(
        variants=[
            V(D.POSTGRESQL, lambda value: sa.func.TO_CHAR(value, "FM999999999990.0999999999")),
        ]
    ),
    base.FuncStrFromBool(
        variants=[
            V(
                D.POSTGRESQL,
                lambda value: sa.case(whens=[(value.is_(None), sa.null()), (value, "True")], else_="False"),
            ),
        ]
    ),
    base.FuncStrFromStrGeo.for_dialect(D.POSTGRESQL),
    base.FuncStrFromDate(
        variants=[
            V(D.POSTGRESQL, lambda value: sa.func.TO_CHAR(value, "YYYY-MM-DD")),
        ]
    ),
    base.FuncStrFromDatetime(
        variants=[
            V(D.POSTGRESQL, lambda value: sa.func.TO_CHAR(value, "YYYY-MM-DD HH24:MI:SS")),
        ]
    ),
    base.FuncStrFromDatetimeTZ(
        variants=[
            # There might be better options; but for now, just str() the naive version.
            # Particularly relevant because in PG and CH the aware value is actually in UTC
            VW(D.POSTGRESQL, ensure_naive_first_arg(n.func.STR)),
        ]
    ),
    base.FuncStrFromString.for_dialect(D.POSTGRESQL),
    base.FuncStrFromUUID(
        variants=[
            V(D.POSTGRESQL, lambda value: sa.cast(value, sa.TEXT)),
        ]
    ),
    base.FuncStrFromArray(
        variants=[
            V(D.POSTGRESQL, lambda value: sa.cast(value, sa.TEXT)),
        ]
    ),
    # tree
    base.FuncTreeStr.for_dialect(D.POSTGRESQL),
]
