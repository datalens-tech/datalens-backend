from sqlalchemy.dialects import mssql as ms_types

from dl_core.db.sa_types_base import (
    make_native_type,
    simple_instantiator,
)


SQLALCHEMY_MSSQL_BASE_TYPES = (
    ms_types.TINYINT,
    ms_types.SMALLINT,
    ms_types.INTEGER,
    ms_types.BIGINT,
    ms_types.FLOAT,
    ms_types.REAL,
    ms_types.NUMERIC,
    ms_types.DECIMAL,
    ms_types.BIT,
    ms_types.CHAR,
    ms_types.VARCHAR,
    ms_types.TEXT,
    ms_types.NCHAR,
    ms_types.NVARCHAR,
    ms_types.NTEXT,
    ms_types.DATE,
    ms_types.DATETIME,
    ms_types.DATETIME2,
    ms_types.SMALLDATETIME,
    ms_types.DATETIMEOFFSET,
    ms_types.UNIQUEIDENTIFIER,
)
SQLALCHEMY_MSSQL_TYPES = {
    make_native_type(typecls): simple_instantiator(typecls)
    for typecls in SQLALCHEMY_MSSQL_BASE_TYPES
}
