import sqlalchemy as sa
import ydb_sqlalchemy as ydb_sa

from dl_formula.core.datatype import DataType
from dl_formula.definitions.base import TranslationVariant
import dl_formula.definitions.functions_type as base
import dl_sqlalchemy_ydb.dialect as ydb_dialect

from dl_connector_ydb.formula.constants import YqlDialect as D


V = TranslationVariant.make

TYPES_SPEC = {
    wlts.name: wlts
    for wlts in [
        base.WhitelistTypeSpec(name="Bool", sa_type=sa.BOOLEAN),
        base.WhitelistTypeSpec(name="Int8", sa_type=ydb_sa.types.Int8),
        base.WhitelistTypeSpec(name="Int16", sa_type=ydb_sa.types.Int16),
        base.WhitelistTypeSpec(name="Int32", sa_type=ydb_sa.types.Int32),
        base.WhitelistTypeSpec(name="Int64", sa_type=ydb_sa.types.Int64),
        base.WhitelistTypeSpec(name="UInt8", sa_type=ydb_sa.types.UInt8),
        base.WhitelistTypeSpec(name="UInt16", sa_type=ydb_sa.types.UInt16),
        base.WhitelistTypeSpec(name="UInt32", sa_type=ydb_sa.types.UInt32),
        base.WhitelistTypeSpec(name="UInt64", sa_type=ydb_sa.types.UInt64),
        base.WhitelistTypeSpec(name="Double", sa_type=ydb_dialect.YqlDouble),
        base.WhitelistTypeSpec(name="Float", sa_type=ydb_dialect.YqlFloat),
        base.WhitelistTypeSpec(name="Decimal", sa_type=sa.DECIMAL, arg_types=base.DECIMAL_CAST_ARG_T),
        base.WhitelistTypeSpec(name="Utf8", sa_type=ydb_dialect.YqlUtf8),
        base.WhitelistTypeSpec(name="String", sa_type=ydb_dialect.YqlString),
        base.WhitelistTypeSpec(name="Uuid", sa_type=ydb_dialect.YqlUuid),
        base.WhitelistTypeSpec(name="Date", sa_type=sa.types.DATE),
        base.WhitelistTypeSpec(name="Datetime", sa_type=ydb_dialect.YqlDateTime),
        base.WhitelistTypeSpec(name="Datetime64", sa_type=ydb_dialect.YqlDateTime64),
        base.WhitelistTypeSpec(name="Timestamp", sa_type=ydb_dialect.YqlTimestamp),
        base.WhitelistTypeSpec(name="Timestamp64", sa_type=ydb_dialect.YqlTimestamp64),
        base.WhitelistTypeSpec(name="Interval", sa_type=ydb_dialect.YqlInterval),
        base.WhitelistTypeSpec(name="Interval64", sa_type=ydb_dialect.YqlInterval64),
    ]
}


class YQLDbCastArgTypes(base.DbCastArgTypes):
    def __init__(self) -> None:
        # See DbCastArgTypes.__init__
        super(base.DbCastArgTypes, self).__init__(
            arg_types=[
                {
                    DataType.BOOLEAN,
                    DataType.INTEGER,
                    DataType.FLOAT,
                    DataType.STRING,
                    DataType.DATE,
                    DataType.ARRAY_INT,
                    DataType.ARRAY_FLOAT,
                    DataType.ARRAY_STR,
                },
                DataType.CONST_STRING,
            ]
        )


class FuncDbCastYQLBase(base.FuncDbCastBase):
    # For numeric types see: https://ydb.tech/docs/en/yql/reference/types/primitive#casting-to-numeric-types
    # Type cast tables date: 2026-01-28.
    #
    # Cast to numeric types
    # Type        Bool    Int8    Int16   Int32   Int64   Uint8     Uint16    Uint32    Uint64     Float   Double  Decimal <- (Target Type)
    # Bool        —       Yes[1]  Yes[1]  Yes[1]  Yes[1]  Yes[1]    Yes[1]    Yes[1]    Yes[1]     Yes[1]  Yes[1]  No
    # Int8        Yes2    —       Yes     Yes     Yes     Yes[3]    Yes[3]    Yes[3]    Yes[3]     Yes     Yes     Yes
    # Int16       Yes2    Yes[4]  —       Yes     Yes     Yes[3,4]  Yes[3]    Yes[3]    Yes[3]     Yes     Yes     Yes
    # Int32       Yes2    Yes[4]  Yes[4]  —       Yes     Yes[3,4]  Yes[3,4]  Yes[3]    Yes[3]     Yes     Yes     Yes
    # Int64       Yes2    Yes[4]  Yes[4]  Yes[4]  —       Yes[3,4]  Yes[3,4]  Yes[3,4]  Yes[3]     Yes     Yes     Yes
    # Uint8       Yes2    Yes[4]  Yes     Yes     Yes     —         Yes       Yes       Yes        Yes     Yes     Yes
    # Uint16      Yes2    Yes[4]  Yes[4]  Yes     Yes     Yes[4]    —         Yes       Yes        Yes     Yes     Yes
    # Uint32      Yes2    Yes[4]  Yes[4]  Yes[4]  Yes     Yes[4]    Yes[4]    —         Yes        Yes     Yes     Yes
    # Uint64      Yes2    Yes[4]  Yes[4]  Yes[4]  Yes[4]  Yes[4]    Yes[4]    Yes[4]    —          Yes     Yes     Yes
    # Float       Yes2    Yes[4]  Yes[4]  Yes[4]  Yes[4]  Yes[3,4]  Yes[3,4]  Yes[3,4]  Yes[3,4]   —       Yes     No
    # Double      Yes2    Yes[4]  Yes[4]  Yes[4]  Yes[4]  Yes[3,4]  Yes[3,4]  Yes[3,4]  Yes[3,4]  Yes      —       No
    # Decimal     No      Yes     Yes     Yes     Yes     Yes       Yes       Yes       Yes       Yes      Yes     —
    # String      Yes     Yes     Yes     Yes     Yes     Yes       Yes       Yes       Yes       Yes      Yes     Yes
    # Utf8        Yes     Yes     Yes     Yes     Yes     Yes       Yes       Yes       Yes       Yes      Yes     Yes
    # Json        No      No      No      No      No      No        No        No        No        No       No      No
    # Yson        Yes[5]  Yes[5]  Yes[5]  Yes[5]  Yes[5]  Yes[5]    Yes[5]    Yes[5]    Yes[5]    Yes[5]   Yes[5]  No
    # Uuid        No      No      No      No      No      No        No        No        No        No       No      No
    # Date        No      Yes[4]  Yes[4]  Yes     Yes     Yes[4]    Yes       Yes       Yes       Yes      Yes     No
    # Datetime    No      Yes[4]  Yes[4]  Yes[4]  Yes     Yes[4]    Yes[4]    Yes       Yes       Yes      Yes     No
    # Datetime64  No      Yes[4]  Yes[4]  Yes[4]  Yes     Yes[4]    Yes[4]    Yes       Yes       Yes      Yes     No
    # Timestamp   No      Yes[4]  Yes[4]  Yes[4]  Yes[4]  Yes[4]    Yes[4]    Yes[4]    Yes       Yes      Yes     No
    # Timestamp64 No      Yes[4]  Yes[4]  Yes[4]  Yes[4]  Yes[4]    Yes[4]    Yes[4]    Yes       Yes      Yes     No
    # Interval    No      Yes[4]  Yes[4]  Yes[4]  Yes     Yes[3,4]  Yes[3,4]  Yes[3,4]  Yes[3]    Yes      Yes     No
    # Interval64  No      Yes[4]  Yes[4]  Yes[4]  Yes     Yes[3,4]  Yes[3,4]  Yes[3,4]  Yes[3]    Yes      Yes     No
    # ^
    # |
    # (Source Type)
    #
    # Cast to time types
    # Type         Date  Date32  Datetime  Datetime64  Timestamp  Timestamp64  Interval  Interval64
    # Bool         No    No      No        No          No         No           No        No
    # Int8         Yes   Yes     Yes       Yes         Yes        Yes          Yes       Yes
    # Int16        Yes   Yes     Yes       Yes         Yes        Yes          Yes       Yes
    # Int32        Yes   Yes     Yes       Yes         Yes        Yes          Yes       Yes
    # Int64        Yes   Yes     Yes       Yes         Yes        Yes          Yes       Yes
    # Uint8        Yes   Yes     Yes       Yes         Yes        Yes          Yes       Yes
    # Uint16       Yes   Yes     Yes       Yes         Yes        Yes          Yes       Yes
    # Uint32       Yes   Yes     Yes       Yes         Yes        Yes          Yes       Yes
    # Uint64       Yes   Yes     Yes       Yes         Yes        Yes          Yes       Yes
    # Float        No    No      No        No          No         No           No        No
    # Double       No    No      No        No          No         No           No        No
    # Decimal      No    No      No        No          No         No           No        No
    # String       Yes   Yes     Yes       Yes         Yes        Yes          Yes       Yes
    # Bytes        Yes   Yes     Yes       Yes         Yes        Yes          Yes       Yes
    # Text         Yes   Yes     Yes       Yes         Yes        Yes          Yes       Yes
    # Json         No    No      No        No          No         No           No        No
    # Yson         No    No      No        No          No         No           No        No
    # Uuid         No    No      No        No          No         No           No        No
    # Date         —     Yes     Yes       Yes         Yes        Yes          No        No
    # Date32       Yes   —       Yes       Yes         Yes        Yes          No        No
    # Datetime     Yes   Yes     —         Yes         Yes        Yes          No        No
    # Datetime64   Yes   Yes     Yes       —           Yes        Yes          No        No
    # Timestamp    Yes   Yes     Yes       Yes         —          Yes          No        No
    # Timestamp64  Yes   Yes     Yes       Yes         Yes        —            No        No
    # Interval     No    No      No        No          No         No           —         Yes
    # Interval64   No    No      No        No          No         No           Yes       —
    #
    # Cast to textual types
    # Type         String  Bytes   Utf8  Text  Json  Yson  Uuid
    # Bool         Yes     Yes     No    No    No    No    No
    # Int8         Yes     Yes     No    No    No    No    No
    # Int16        Yes     Yes     No    No    No    No    No
    # Int32        Yes     Yes     No    No    No    No    No
    # Int64        Yes     Yes     No    No    No    No    No
    # Uint8        Yes     Yes     No    No    No    No    No
    # Uint16       Yes     Yes     No    No    No    No    No
    # Uint32       Yes     Yes     No    No    No    No    No
    # Uint64       Yes     Yes     No    No    No    No    No
    # Float        Yes     Yes     No    No    No    No    No
    # Double       Yes     Yes     No    No    No    No    No
    # Decimal      Yes     Yes     No    No    No    No    No
    # String       —       —       Yes   Yes   Yes   Yes   Yes
    # Bytes        —       —       Yes   Yes   Yes   Yes   Yes
    # Utf8         Yes     Yes     —     —     No    No    No
    # Text         Yes     Yes     —     —     No    No    No
    # Json         Yes     Yes     Yes   Yes   —     No    No
    # Yson         Yes[1]  Yes[1]  No    No    No    —     No
    # Uuid         Yes     Yes     Yes   Yes   No    No    —
    # Date         Yes     Yes     Yes   Yes   No    No    No
    # Datetime     Yes     Yes     Yes   Yes   No    No    No
    # Timestamp    Yes     Yes     Yes   Yes   No    No    No
    # Interval     Yes     Yes     Yes   Yes   No    No    No
    # Date32       Yes     Yes     Yes   Yes   No    No    No
    # Datetime64   Yes     Yes     Yes   Yes   No    No    No
    # Timestamp64  Yes     Yes     Yes   Yes   No    No    No
    # Interval64   Yes     Yes     Yes   Yes   No    No    No
    #
    # [1] - True is converted to 1 and False to 0.
    # [2] - Any value other than 0 is converted to True, 0 is converted to False.
    # [3] - Possible only in case of a non-negative value.
    # [4] - Possible only within the valid range.
    # [5] - Using the built-in function Yson::ConvertTo.

    argument_types = [
        YQLDbCastArgTypes(),
    ]

    WHITELISTS = {
        yql_dialect: {
            # TODO: Decimal, DyNumber, Json, JsonDocument, Yson, Uuid
            # All mappings are based on the table above.
            # Notes:
            # - Interval can not be casted to Bool
            # - Interval can not be casted to Decimal
            # - Utf8 can not be casted to Uuid
            DataType.BOOLEAN: [
                # > Bool
                TYPES_SPEC["Bool"],
                # > INT
                TYPES_SPEC["Int8"],
                TYPES_SPEC["Int16"],
                TYPES_SPEC["Int32"],
                TYPES_SPEC["Int64"],
                # > UINT
                TYPES_SPEC["UInt8"],
                TYPES_SPEC["UInt16"],
                TYPES_SPEC["UInt32"],
                TYPES_SPEC["UInt64"],
                # > Float
                TYPES_SPEC["Float"],
                # > Double
                TYPES_SPEC["Double"],
                # > String
                TYPES_SPEC["String"],
            ],
            DataType.INTEGER: [
                # > Bool
                TYPES_SPEC["Bool"],
                # > INT
                TYPES_SPEC["Int8"],
                TYPES_SPEC["Int16"],
                TYPES_SPEC["Int32"],
                TYPES_SPEC["Int64"],
                # > UINT
                TYPES_SPEC["UInt8"],
                TYPES_SPEC["UInt16"],
                TYPES_SPEC["UInt32"],
                TYPES_SPEC["UInt64"],
                # > Float
                TYPES_SPEC["Float"],
                # > Double
                TYPES_SPEC["Double"],
                # > Decimal
                TYPES_SPEC["Decimal"],
                # > String
                TYPES_SPEC["String"],
                # > Date
                TYPES_SPEC["Date"],
                # > Datetime
                TYPES_SPEC["Datetime"],
                TYPES_SPEC["Datetime64"],
                # > Timestamp
                TYPES_SPEC["Timestamp"],
                TYPES_SPEC["Timestamp64"],
                # > Interval
                TYPES_SPEC["Interval"],
                TYPES_SPEC["Interval64"],
            ],
            DataType.FLOAT: [
                # > Bool
                TYPES_SPEC["Bool"],
                # > INT
                TYPES_SPEC["Int8"],
                TYPES_SPEC["Int16"],
                TYPES_SPEC["Int32"],
                TYPES_SPEC["Int64"],
                # > UINT
                TYPES_SPEC["UInt8"],
                TYPES_SPEC["UInt16"],
                TYPES_SPEC["UInt32"],
                TYPES_SPEC["UInt64"],
                # > Float
                TYPES_SPEC["Float"],
                # > Double
                TYPES_SPEC["Double"],
                # > String
                TYPES_SPEC["String"],
            ],
            DataType.STRING: [
                # > Bool
                TYPES_SPEC["Bool"],
                # > INT
                TYPES_SPEC["Int8"],
                TYPES_SPEC["Int16"],
                TYPES_SPEC["Int32"],
                TYPES_SPEC["Int64"],
                # > UINT
                TYPES_SPEC["UInt8"],
                TYPES_SPEC["UInt16"],
                TYPES_SPEC["UInt32"],
                TYPES_SPEC["UInt64"],
                # > Float
                TYPES_SPEC["Float"],
                # > Double
                TYPES_SPEC["Double"],
                # > Decimal
                TYPES_SPEC["Decimal"],
                # > String
                TYPES_SPEC["String"],
                # > Utf8
                TYPES_SPEC["Utf8"],
                # > Date
                TYPES_SPEC["Date"],
                # > Datetime
                TYPES_SPEC["Datetime"],
                TYPES_SPEC["Datetime64"],
                # > Timestamp
                TYPES_SPEC["Timestamp"],
                TYPES_SPEC["Timestamp64"],
                # > Interval
                TYPES_SPEC["Interval"],
                TYPES_SPEC["Interval64"],
                # > Uuid
                TYPES_SPEC["Uuid"],
            ],
            DataType.DATE: [
                # > INT
                TYPES_SPEC["Int8"],
                TYPES_SPEC["Int16"],
                TYPES_SPEC["Int32"],
                TYPES_SPEC["Int64"],
                # > UINT
                TYPES_SPEC["UInt8"],
                TYPES_SPEC["UInt16"],
                TYPES_SPEC["UInt32"],
                TYPES_SPEC["UInt64"],
                # > Float
                TYPES_SPEC["Float"],
                # > Double
                TYPES_SPEC["Double"],
                # > String
                TYPES_SPEC["String"],
                # > Utf8
                TYPES_SPEC["Utf8"],
                # > Date
                TYPES_SPEC["Date"],
                # > Datetime
                TYPES_SPEC["Datetime"],
                TYPES_SPEC["Datetime64"],
                # > Timestamp
                TYPES_SPEC["Timestamp"],
                TYPES_SPEC["Timestamp64"],
            ],
            DataType.DATETIME: [
                # > INT
                TYPES_SPEC["Int8"],
                TYPES_SPEC["Int16"],
                TYPES_SPEC["Int32"],
                TYPES_SPEC["Int64"],
                # > UINT
                TYPES_SPEC["UInt8"],
                TYPES_SPEC["UInt16"],
                TYPES_SPEC["UInt32"],
                TYPES_SPEC["UInt64"],
                # > Float
                TYPES_SPEC["Float"],
                # > Double
                TYPES_SPEC["Double"],
                # > String
                TYPES_SPEC["String"],
                # > Utf8
                TYPES_SPEC["Utf8"],
                # > Date
                TYPES_SPEC["Date"],
                # > Datetime
                TYPES_SPEC["Datetime"],
                TYPES_SPEC["Datetime64"],
                # > Timestamp
                TYPES_SPEC["Timestamp"],
                TYPES_SPEC["Timestamp64"],
            ],
            DataType.GENERICDATETIME: [
                # > INT
                TYPES_SPEC["Int8"],
                TYPES_SPEC["Int16"],
                TYPES_SPEC["Int32"],
                TYPES_SPEC["Int64"],
                # > UINT
                TYPES_SPEC["UInt8"],
                TYPES_SPEC["UInt16"],
                TYPES_SPEC["UInt32"],
                TYPES_SPEC["UInt64"],
                # > Float
                TYPES_SPEC["Float"],
                # > Double
                TYPES_SPEC["Double"],
                # > String
                TYPES_SPEC["String"],
                # > Utf8
                TYPES_SPEC["Utf8"],
                # > Date
                TYPES_SPEC["Date"],
                # > Datetime
                TYPES_SPEC["Datetime"],
                TYPES_SPEC["Datetime64"],
                # > Timestamp
                TYPES_SPEC["Timestamp"],
                TYPES_SPEC["Timestamp64"],
            ],
            DataType.ARRAY_STR: [],
            DataType.ARRAY_INT: [],
            DataType.ARRAY_FLOAT: [],
        }
        for yql_dialect in (D.YQL, D.YQ, D.YDB)
    }


class FuncDbCastYQL2(FuncDbCastYQLBase, base.FuncDbCast2):
    pass


class FuncDbCastYQL3(FuncDbCastYQLBase, base.FuncDbCast3):
    pass


class FuncDbCastYQL4(FuncDbCastYQLBase, base.FuncDbCast4):
    pass


DEFINITIONS_TYPE = [
    # bool
    base.FuncBoolFromNull.for_dialect(D.YQL),
    base.FuncBoolFromNumber.for_dialect(D.YQL),
    base.FuncBoolFromBool.for_dialect(D.YQL),
    base.FuncBoolFromStrGeo.for_dialect(D.YQL),
    base.FuncBoolFromDateDatetime.for_dialect(D.YQL),
    # date
    base.FuncDate1FromNull.for_dialect(D.YQL),
    base.FuncDate1FromDatetime.for_dialect(D.YQL),
    base.FuncDate1FromString.for_dialect(D.YQL),
    base.FuncDate1FromNumber(
        variants=[
            V(
                D.YQL, lambda expr: sa.cast(sa.cast(sa.cast(expr, sa.BIGINT), sa.DATETIME), sa.DATE)
            ),  # number -> dt -> date
        ]
    ),
    # datetime
    base.FuncDatetime1FromNull.for_dialect(D.YQL),
    base.FuncDatetime1FromDatetime.for_dialect(D.YQL),
    base.FuncDatetime1FromDate.for_dialect(D.YQL),
    base.FuncDatetime1FromNumber(
        variants=[
            V(D.YQL, lambda expr: sa.cast(sa.cast(expr, sa.BIGINT), sa.DateTime())),
        ]
    ),
    base.FuncDatetime1FromString(
        variants=[
            # e.g. `DateTime::MakeDatetime(DateTime::ParseIso8601('2021-06-01 18:00:59')) as c`
            V(D.YQL, lambda expr: sa.func.DateTime.MakeDatetime(sa.func.DateTime.ParseIso8601(expr))),
        ]
    ),
    # datetimetz
    base.FuncDatetimeTZConst.for_dialect(D.YQL),
    # db_cast
    FuncDbCastYQL2(),
    FuncDbCastYQL3(),
    FuncDbCastYQL4(),
    # float
    base.FuncFloatNumber(
        variants=[
            V(D.YQL, lambda value: sa.cast(value, sa.FLOAT)),  # TODO: need it to become SQL `CAST(… AS DOUBLE)`.
        ]
    ),
    base.FuncFloatString(
        variants=[
            V(D.YQL, lambda value: sa.cast(value, sa.FLOAT)),
        ]
    ),
    base.FuncFloatFromBool(
        variants=[
            V(D.YQL, lambda value: sa.cast(value, sa.FLOAT)),
        ]
    ),
    base.FuncFloatFromDate(
        variants=[
            V(D.YQL, lambda expr: sa.cast(sa.cast(expr, sa.DATETIME), sa.FLOAT)),  # date -> dt -> number
        ]
    ),
    base.FuncFloatFromDatetime(
        variants=[
            V(D.YQL, lambda expr: sa.cast(expr, sa.FLOAT)),
        ]
    ),
    base.FuncFloatFromGenericDatetime(
        variants=[
            V(D.YQL, lambda expr: sa.cast(expr, sa.FLOAT)),
        ]
    ),
    # genericdatetime
    base.FuncGenericDatetime1FromNull.for_dialect(D.YQL),
    base.FuncGenericDatetime1FromDatetime.for_dialect(D.YQL),
    base.FuncGenericDatetime1FromDate.for_dialect(D.YQL),
    base.FuncGenericDatetime1FromNumber(
        variants=[
            V(D.YQL, lambda expr: sa.cast(sa.cast(expr, sa.BIGINT), sa.DateTime())),
        ]
    ),
    base.FuncGenericDatetime1FromString(
        variants=[
            # e.g. `DateTime::MakeDatetime(DateTime::ParseIso8601('2021-06-01 18:00:59')) as c`
            V(D.YQL, lambda expr: sa.func.DateTime.MakeDatetime(sa.func.DateTime.ParseIso8601(expr))),
        ]
    ),
    # geopoint
    base.FuncGeopointFromStr.for_dialect(D.YQL),
    base.FuncGeopointFromCoords.for_dialect(D.YQL),
    # geopolygon
    base.FuncGeopolygon.for_dialect(D.YQL),
    # int
    base.FuncIntFromNull(
        variants=[
            V(D.YQL, lambda _: sa.cast(sa.null(), sa.BIGINT())),
        ]
    ),
    base.FuncIntFromInt.for_dialect(D.YQL),
    base.FuncIntFromFloat(
        variants=[
            V(D.YQL, lambda value: sa.cast(value, sa.BIGINT())),
        ]
    ),
    base.FuncIntFromBool(
        variants=[
            V(D.YQL, lambda value: sa.cast(value, sa.BIGINT)),
        ]
    ),
    base.FuncIntFromStr(
        variants=[
            V(D.YQL, lambda expr: sa.func.cast(expr, sa.BIGINT)),
        ]
    ),
    base.FuncIntFromDate(
        variants=[
            V(D.YQL, lambda expr: sa.cast(sa.cast(expr, sa.DATETIME), sa.BIGINT)),
        ]
    ),
    base.FuncIntFromDatetime(
        variants=[
            V(D.YQL, lambda expr: sa.cast(expr, sa.BIGINT)),
        ]
    ),
    base.FuncIntFromGenericDatetime(
        variants=[
            V(D.YQL, lambda expr: sa.cast(expr, sa.BIGINT)),
        ]
    ),
    # str
    base.FuncStrFromNull(
        variants=[
            V(D.YQL, lambda value: sa.cast(sa.null(), sa.TEXT)),
        ]
    ),
    base.FuncStrFromUnsupported(
        variants=[
            # YQL: uncertain.
            # Does not work for e.g. arrays:
            # V(D.YQL, lambda value: sa.cast(value, sa.TEXT)),
            # Does not work for e.g. Decimal:
            V(
                D.YQL,
                lambda value: sa.cast(sa.func.ToBytes(sa.func.Yson.SerializePretty(sa.func.Yson.From(value))), sa.TEXT),
            ),
        ]
    ),
    base.FuncStrFromInteger(
        variants=[
            V(D.YQL, lambda value: sa.cast(value, sa.TEXT)),
        ]
    ),
    base.FuncStrFromFloat(
        variants=[
            V(D.YQL, lambda value: sa.cast(value, sa.TEXT)),
        ]
    ),
    base.FuncStrFromBool(
        variants=[
            V(D.YQL, lambda value: sa.case(whens=[(value.is_(None), sa.null()), (value, "True")], else_="False")),
        ]
    ),
    base.FuncStrFromStrGeo.for_dialect(D.YQL),
    base.FuncStrFromDate(
        variants=[
            V(D.YQL, lambda value: sa.cast(value, sa.TEXT)),
        ]
    ),
    base.FuncStrFromDatetime(
        variants=[
            V(D.YQL, lambda value: sa.cast(value, sa.TEXT)),  # results in e.g. "2021-06-01T15:20:24Z"
        ]
    ),
    base.FuncStrFromString.for_dialect(D.YQL),
    base.FuncStrFromUUID(
        variants=[
            V(D.YQL, lambda value: sa.cast(value, sa.TEXT)),
        ]
    ),
]
