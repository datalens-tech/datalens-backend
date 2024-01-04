from snowflake import sqlalchemy as ssa

from dl_constants.enums import UserDataType
from dl_core.db.conversion_base import (
    TypeTransformer,
    make_native_type,
)


SNOW_TYPES_INT = frozenset(
    (
        ssa.BIGINT,
        ssa.SMALLINT,
        ssa.INTEGER,
        ssa.TINYINT,
        ssa.SMALLINT,
    )
)
SNOW_TYPES_FLOAT = frozenset(
    (
        ssa.FLOAT,
        ssa.DECIMAL,
        ssa.DEC,
        ssa.DOUBLE,
        ssa.REAL,
    )
)
SNOW_TYPES_STRING = frozenset(
    (
        ssa.STRING,
        ssa.VARCHAR,
        ssa.CHAR,
        ssa.CHARACTER,
        ssa.TEXT,
    )
)


class SnowFlakeTypeTransformer(TypeTransformer):
    # https://docs.snowflake.com/en/sql-reference/data-types.html

    native_to_user_map = {
        make_native_type(ssa.DATE): UserDataType.date,
        make_native_type(ssa.DATETIME): UserDataType.genericdatetime,
        **{make_native_type(t): UserDataType.integer for t in SNOW_TYPES_INT},
        **{make_native_type(t): UserDataType.float for t in SNOW_TYPES_FLOAT},
        **{make_native_type(t): UserDataType.string for t in SNOW_TYPES_STRING},
        make_native_type(ssa.BOOLEAN): UserDataType.boolean,
        # todo: review datetime/genericdatime/datetimetz/timestamp
        # todo: array, geo
    }
    user_to_native_map = {
        UserDataType.date: make_native_type(ssa.DATE),
        UserDataType.genericdatetime: make_native_type(ssa.DATETIME),
        UserDataType.integer: make_native_type(ssa.INT),
        UserDataType.string: make_native_type(ssa.STRING),
        UserDataType.boolean: make_native_type(ssa.BOOLEAN),
    }
    casters = {
        **TypeTransformer.casters,
        # UserDataType.date: GSheetsDateTypeCaster(),
        # UserDataType.datetime: GSheetsDatetimeTypeCaster(),
        # UserDataType.genericdatetime: GSheetsGenericDatetimeTypeCaster(),
    }
