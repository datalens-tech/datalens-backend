import sqlalchemy as sa
from sqlalchemy.dialects import mssql as ms_types

from dl_constants.enums import UserDataType
from dl_core.db.conversion_base import (
    LowercaseTypeCaster,
    TypeTransformer,
    make_native_type,
)

from bi_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL


class MSSQLServerTypeTransformer(TypeTransformer):
    conn_type = CONNECTION_TYPE_MSSQL
    native_to_user_map = {
        **{
            make_native_type(CONNECTION_TYPE_MSSQL, t): UserDataType.integer  # type: ignore  # TODO: fix
            for t in (ms_types.TINYINT, ms_types.SMALLINT, ms_types.INTEGER, ms_types.BIGINT)
        },
        **{
            make_native_type(CONNECTION_TYPE_MSSQL, t): UserDataType.float
            for t in (ms_types.FLOAT, ms_types.REAL, ms_types.NUMERIC, ms_types.DECIMAL)
        },
        make_native_type(CONNECTION_TYPE_MSSQL, ms_types.BIT): UserDataType.boolean,
        **{
            make_native_type(CONNECTION_TYPE_MSSQL, t): UserDataType.string
            for t in (
                ms_types.CHAR,
                ms_types.VARCHAR,
                ms_types.TEXT,
                ms_types.NCHAR,
                ms_types.NVARCHAR,
                ms_types.NTEXT,
            )
        },
        make_native_type(CONNECTION_TYPE_MSSQL, ms_types.DATE): UserDataType.date,
        **{
            make_native_type(CONNECTION_TYPE_MSSQL, t): UserDataType.genericdatetime
            for t in (ms_types.DATETIME, ms_types.DATETIME2, ms_types.SMALLDATETIME, ms_types.DATETIMEOFFSET)
        },
        make_native_type(CONNECTION_TYPE_MSSQL, ms_types.UNIQUEIDENTIFIER): UserDataType.uuid,
        make_native_type(CONNECTION_TYPE_MSSQL, sa.sql.sqltypes.NullType): UserDataType.unsupported,
    }
    user_to_native_map = {
        UserDataType.integer: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.BIGINT),
        UserDataType.float: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.FLOAT),
        UserDataType.boolean: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.BIT),
        UserDataType.string: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.VARCHAR),
        UserDataType.date: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.DATE),
        UserDataType.datetime: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.DATETIME),
        UserDataType.genericdatetime: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.DATETIME),
        UserDataType.geopoint: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.VARCHAR),
        UserDataType.geopolygon: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.VARCHAR),
        UserDataType.uuid: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.UNIQUEIDENTIFIER),
        UserDataType.markup: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.VARCHAR),
        UserDataType.unsupported: make_native_type(CONNECTION_TYPE_MSSQL, sa.sql.sqltypes.NullType),
    }
    casters = {
        **TypeTransformer.casters,  # type: ignore  # TODO: fix
        UserDataType.uuid: LowercaseTypeCaster(),
    }
