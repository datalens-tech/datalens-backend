from dl_constants.enums import (
    ConnectionType,
    CreateDSFrom,
    SourceBackendType,
)


BACKEND_TYPE_MYSQL = SourceBackendType.declare("MYSQL")
CONNECTION_TYPE_MYSQL = ConnectionType.declare("mysql")
SOURCE_TYPE_MYSQL_TABLE = CreateDSFrom.declare("MYSQL_TABLE")
SOURCE_TYPE_MYSQL_SUBSELECT = CreateDSFrom.declare("MYSQL_SUBSELECT")
