from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
    SourceBackendType,
)


BACKEND_TYPE_GSHEETS = SourceBackendType.declare("GSHEETS")
CONNECTION_TYPE_GSHEETS = ConnectionType.declare("gsheets")
SOURCE_TYPE_GSHEETS = DataSourceType.declare("GSHEETS")
