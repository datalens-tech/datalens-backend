from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
    SourceBackendType,
)


BACKEND_TYPE_MSSQL = SourceBackendType.declare("MSSQL")
CONNECTION_TYPE_MSSQL = ConnectionType.declare("mssql")
SOURCE_TYPE_MSSQL_TABLE = DataSourceType.declare("MSSQL_TABLE")
SOURCE_TYPE_MSSQL_SUBSELECT = DataSourceType.declare("MSSQL_SUBSELECT")
