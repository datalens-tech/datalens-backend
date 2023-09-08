from bi_constants.enums import SourceBackendType, CreateDSFrom, ConnectionType


BACKEND_TYPE_MONITORING = SourceBackendType.declare('MONITORING')
CONNECTION_TYPE_MONITORING = ConnectionType.monitoring  # FIXME: declaration
SOURCE_TYPE_MONITORING = CreateDSFrom.declare('MONITORING')
