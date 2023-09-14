from bi_constants.enums import (
    ConnectionType,
    CreateDSFrom,
    SourceBackendType,
)

BACKEND_TYPE_PROMQL = SourceBackendType.declare("PROMQL")
CONNECTION_TYPE_PROMQL = ConnectionType.promql  # FIXME: declaration
SOURCE_TYPE_PROMQL = CreateDSFrom.declare("PROMQL")
