from sqlalchemy.dialects import mssql as ms_types

from dl_type_transformer.sa_types_base import simple_instantiator
from dl_type_transformer.type_transformer import make_native_type

from dl_connector_mssql.core.constants import BACKEND_TYPE_MSSQL


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
    (BACKEND_TYPE_MSSQL, make_native_type(typecls)): simple_instantiator(typecls)
    for typecls in SQLALCHEMY_MSSQL_BASE_TYPES
}
