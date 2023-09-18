import sqlalchemy as sa
from sqlalchemy.dialects.oracle import base as or_types  # not all data types are imported in init in older SA versions

from dl_constants.enums import BIType
from dl_core.db.conversion_base import (
    TypeTransformer,
    make_native_type,
)

from bi_connector_oracle.core.constants import CONNECTION_TYPE_ORACLE


class OracleServerTypeTransformer(TypeTransformer):
    conn_type = CONNECTION_TYPE_ORACLE
    native_to_user_map = {
        # No separate type for integer. Number acts as DECIMAL with customizable precision.
        **{
            make_native_type(CONNECTION_TYPE_ORACLE, t): BIType.integer
            for t in (sa.Integer,)  # pseudo type used if scale == 0
        },
        **{
            make_native_type(CONNECTION_TYPE_ORACLE, t): BIType.float  # type: ignore  # TODO: fix
            for t in (or_types.NUMBER, or_types.BINARY_FLOAT, or_types.BINARY_DOUBLE)
        },
        **{
            make_native_type(CONNECTION_TYPE_ORACLE, t): BIType.string
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
            make_native_type(CONNECTION_TYPE_ORACLE, t): BIType.genericdatetime  # type: ignore  # TODO: fix
            for t in (or_types.DATE, or_types.TIMESTAMP)
        },
        # No separate type for date, it's the same as for datetime
        make_native_type(CONNECTION_TYPE_ORACLE, sa.sql.sqltypes.NullType): BIType.unsupported,
    }
    user_to_native_map = {
        BIType.integer: make_native_type(CONNECTION_TYPE_ORACLE, sa.INTEGER),
        BIType.float: make_native_type(CONNECTION_TYPE_ORACLE, or_types.BINARY_DOUBLE),
        BIType.boolean: make_native_type(CONNECTION_TYPE_ORACLE, or_types.NUMBER),
        BIType.string: make_native_type(CONNECTION_TYPE_ORACLE, or_types.NVARCHAR2),
        BIType.date: make_native_type(CONNECTION_TYPE_ORACLE, or_types.DATE),
        BIType.datetime: make_native_type(CONNECTION_TYPE_ORACLE, or_types.DATE),
        BIType.genericdatetime: make_native_type(CONNECTION_TYPE_ORACLE, or_types.DATE),
        BIType.geopoint: make_native_type(CONNECTION_TYPE_ORACLE, or_types.NVARCHAR2),
        BIType.geopolygon: make_native_type(CONNECTION_TYPE_ORACLE, or_types.NVARCHAR2),
        BIType.uuid: make_native_type(CONNECTION_TYPE_ORACLE, or_types.NVARCHAR2),
        BIType.markup: make_native_type(CONNECTION_TYPE_ORACLE, or_types.NVARCHAR2),
        BIType.unsupported: make_native_type(CONNECTION_TYPE_ORACLE, sa.sql.sqltypes.NullType),
    }
