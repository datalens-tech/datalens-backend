from snowflake import sqlalchemy as ssa

from dl_connector_snowflake.core.constants import CONNECTION_TYPE_SNOWFLAKE
from dl_constants.enums import BIType
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

    conn_type = CONNECTION_TYPE_SNOWFLAKE
    native_to_user_map = {
        make_native_type(CONNECTION_TYPE_SNOWFLAKE, ssa.DATE): BIType.date,
        make_native_type(CONNECTION_TYPE_SNOWFLAKE, ssa.DATETIME): BIType.genericdatetime,
        **{make_native_type(CONNECTION_TYPE_SNOWFLAKE, t): BIType.integer for t in SNOW_TYPES_INT},
        **{make_native_type(CONNECTION_TYPE_SNOWFLAKE, t): BIType.float for t in SNOW_TYPES_FLOAT},
        **{make_native_type(CONNECTION_TYPE_SNOWFLAKE, t): BIType.string for t in SNOW_TYPES_STRING},
        make_native_type(CONNECTION_TYPE_SNOWFLAKE, ssa.BOOLEAN): BIType.boolean,
        # todo: review datetime/genericdatime/datetimetz/timestamp
        # todo: array, geo
    }
    user_to_native_map = {
        BIType.date: make_native_type(CONNECTION_TYPE_SNOWFLAKE, ssa.DATE),
        BIType.genericdatetime: make_native_type(CONNECTION_TYPE_SNOWFLAKE, ssa.DATETIME),
        BIType.integer: make_native_type(CONNECTION_TYPE_SNOWFLAKE, ssa.INT),
        BIType.string: make_native_type(CONNECTION_TYPE_SNOWFLAKE, ssa.STRING),
        BIType.boolean: make_native_type(CONNECTION_TYPE_SNOWFLAKE, ssa.BOOLEAN),
    }
    casters = {
        **TypeTransformer.casters,
        # BIType.date: GSheetsDateTypeCaster(),
        # BIType.datetime: GSheetsDatetimeTypeCaster(),
        # BIType.genericdatetime: GSheetsGenericDatetimeTypeCaster(),
    }
