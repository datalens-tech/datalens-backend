from dl_constants.enums import SourceBackendType, CreateDSFrom, ConnectionType


BACKEND_TYPE_YQ = SourceBackendType.declare('YQ')

CONNECTION_TYPE_YQ = ConnectionType.declare('yq')

SOURCE_TYPE_YQ_TABLE = CreateDSFrom.declare('YQ_TABLE')
SOURCE_TYPE_YQ_SUBSELECT = CreateDSFrom.declare('YQ_SUBSELECT')
