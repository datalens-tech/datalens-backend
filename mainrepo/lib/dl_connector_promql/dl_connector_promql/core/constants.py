from dl_constants.enums import (
    ConnectionType,
    CreateDSFrom,
    SourceBackendType,
)


BACKEND_TYPE_PROMQL = SourceBackendType.declare("PROMQL")
CONNECTION_TYPE_PROMQL = ConnectionType.declare("promql")
SOURCE_TYPE_PROMQL = CreateDSFrom.declare("PROMQL")
