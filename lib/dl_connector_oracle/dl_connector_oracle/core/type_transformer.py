import sqlalchemy as sa
from sqlalchemy.dialects.oracle import base as or_types  # not all data types are imported in init in older SA versions

from dl_constants.enums import UserDataType
from dl_core.db.conversion_base import (
    TypeTransformer,
    make_native_type,
)


class OracleServerTypeTransformer(TypeTransformer):
    native_to_user_map = {
        # No separate type for integer. Number acts as DECIMAL with customizable precision.
        **{
            make_native_type(t): UserDataType.integer
            for t in (sa.Integer,)  # pseudo type used if scale == 0
        },
        **{
            make_native_type(t): UserDataType.float  # type: ignore  # TODO: fix
            for t in (or_types.NUMBER, or_types.BINARY_FLOAT, or_types.BINARY_DOUBLE)
        },
        **{
            make_native_type(t): UserDataType.string
            for t in (
                or_types.CHAR,
                or_types.VARCHAR,
                or_types.VARCHAR2,
                sa.NCHAR,
                or_types.NVARCHAR,
                or_types.NVARCHAR2,
            )
        },
        **{
            make_native_type(t): UserDataType.genericdatetime  # type: ignore  # TODO: fix
            for t in (or_types.DATE, or_types.TIMESTAMP)
        },
        # No separate type for date, it's the same as for datetime
        make_native_type(sa.sql.sqltypes.NullType): UserDataType.unsupported,
    }
    user_to_native_map = {
        UserDataType.integer: make_native_type(sa.INTEGER),
        UserDataType.float: make_native_type(or_types.BINARY_DOUBLE),
        UserDataType.boolean: make_native_type(or_types.NUMBER),
        UserDataType.string: make_native_type(or_types.NVARCHAR2),
        UserDataType.date: make_native_type(or_types.DATE),
        UserDataType.datetime: make_native_type(or_types.DATE),
        UserDataType.genericdatetime: make_native_type(or_types.DATE),
        UserDataType.geopoint: make_native_type(or_types.NVARCHAR2),
        UserDataType.geopolygon: make_native_type(or_types.NVARCHAR2),
        UserDataType.uuid: make_native_type(or_types.NVARCHAR2),
        UserDataType.markup: make_native_type(or_types.NVARCHAR2),
        UserDataType.unsupported: make_native_type(sa.sql.sqltypes.NullType),
    }
