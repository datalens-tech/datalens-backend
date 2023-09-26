from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
    SourceBackendType,
)


BACKEND_TYPE_MYSQL = SourceBackendType.declare("MYSQL")
CONNECTION_TYPE_MYSQL = ConnectionType.declare("mysql")
SOURCE_TYPE_MYSQL_TABLE = DataSourceType.declare("MYSQL_TABLE")
SOURCE_TYPE_MYSQL_SUBSELECT = DataSourceType.declare("MYSQL_SUBSELECT")
