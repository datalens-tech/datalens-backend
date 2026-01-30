from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
    SourceBackendType,
)


BACKEND_TYPE_STARROCKS = SourceBackendType.declare("STARROCKS")
CONNECTION_TYPE_STARROCKS = ConnectionType.declare("starrocks")
SOURCE_TYPE_STARROCKS_TABLE = DataSourceType.declare("STARROCKS_TABLE")
SOURCE_TYPE_STARROCKS_SUBSELECT = DataSourceType.declare("STARROCKS_SUBSELECT")
