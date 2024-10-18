from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
    SourceBackendType,
)


BACKEND_TYPE_GSHEETS_V2 = SourceBackendType.declare("GSHEETS_V2")
CONNECTION_TYPE_GSHEETS_V2 = ConnectionType.declare("gsheets_v2")
SOURCE_TYPE_GSHEETS_V2 = DataSourceType.declare("GSHEETS_V2")
