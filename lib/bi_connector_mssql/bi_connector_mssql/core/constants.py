from dl_constants.enums import (
    ConnectionType,
    CreateDSFrom,
    SourceBackendType,
)


BACKEND_TYPE_MSSQL = SourceBackendType.declare("MSSQL")
CONNECTION_TYPE_MSSQL = ConnectionType.declare("mssql")
SOURCE_TYPE_MSSQL_TABLE = CreateDSFrom.declare("MSSQL_TABLE")
SOURCE_TYPE_MSSQL_SUBSELECT = CreateDSFrom.declare("MSSQL_SUBSELECT")
