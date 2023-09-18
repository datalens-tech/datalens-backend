import sqlalchemy as sa
from sqlalchemy.dialects import mssql as ms_types

from dl_constants.enums import BIType

from dl_core.db.conversion_base import LowercaseTypeCaster, TypeTransformer, make_native_type

from bi_connector_mssql.core.constants import CONNECTION_TYPE_MSSQL


class MSSQLServerTypeTransformer(TypeTransformer):
    conn_type = CONNECTION_TYPE_MSSQL
    native_to_user_map = {
        **{
            make_native_type(CONNECTION_TYPE_MSSQL, t): BIType.integer  # type: ignore  # TODO: fix
            for t in (
                ms_types.TINYINT, ms_types.SMALLINT, ms_types.INTEGER, ms_types.BIGINT
            )
        },
        **{
            make_native_type(CONNECTION_TYPE_MSSQL, t): BIType.float
            for t in (
                ms_types.FLOAT, ms_types.REAL, ms_types.NUMERIC, ms_types.DECIMAL
            )
        },
        make_native_type(CONNECTION_TYPE_MSSQL, ms_types.BIT): BIType.boolean,
        **{
            make_native_type(CONNECTION_TYPE_MSSQL, t): BIType.string
            for t in (
                ms_types.CHAR, ms_types.VARCHAR, ms_types.TEXT,
                ms_types.NCHAR, ms_types.NVARCHAR, ms_types.NTEXT,
            )
        },
        make_native_type(CONNECTION_TYPE_MSSQL, ms_types.DATE): BIType.date,
        **{make_native_type(CONNECTION_TYPE_MSSQL, t): BIType.genericdatetime for t in (
            ms_types.DATETIME, ms_types.DATETIME2, ms_types.SMALLDATETIME, ms_types.DATETIMEOFFSET
        )},
        make_native_type(CONNECTION_TYPE_MSSQL, ms_types.UNIQUEIDENTIFIER): BIType.uuid,
        make_native_type(CONNECTION_TYPE_MSSQL, sa.sql.sqltypes.NullType): BIType.unsupported,
    }
    user_to_native_map = {
        BIType.integer: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.BIGINT),
        BIType.float: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.FLOAT),
        BIType.boolean: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.BIT),
        BIType.string: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.VARCHAR),
        BIType.date: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.DATE),
        BIType.datetime: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.DATETIME),
        BIType.genericdatetime: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.DATETIME),
        BIType.geopoint: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.VARCHAR),
        BIType.geopolygon: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.VARCHAR),
        BIType.uuid: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.UNIQUEIDENTIFIER),
        BIType.markup: make_native_type(CONNECTION_TYPE_MSSQL, ms_types.VARCHAR),
        BIType.unsupported: make_native_type(CONNECTION_TYPE_MSSQL, sa.sql.sqltypes.NullType),
    }
    casters = {
        **TypeTransformer.casters,  # type: ignore  # TODO: fix
        BIType.uuid: LowercaseTypeCaster(),
    }
