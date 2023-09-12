from bi_constants.enums import ConnectionType, SourceBackendType, CreateDSFrom


BACKEND_TYPE_GSHEETS = SourceBackendType.declare('GSHEETS')
CONNECTION_TYPE_GSHEETS = ConnectionType.gsheets  # FIXME: declaration
SOURCE_TYPE_GSHEETS = CreateDSFrom.declare('GSHEETS')
