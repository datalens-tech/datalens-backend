from dl_constants.enums import (
    ConnectionType,
    CreateDSFrom,
    SourceBackendType,
)

BACKEND_TYPE_MONITORING = SourceBackendType.declare("MONITORING")
CONNECTION_TYPE_MONITORING = ConnectionType.declare("monitoring")
SOURCE_TYPE_MONITORING = CreateDSFrom.declare("MONITORING")
