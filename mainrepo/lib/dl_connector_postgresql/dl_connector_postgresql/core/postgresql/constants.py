from dl_constants.enums import (
    ConnectionType,
    CreateDSFrom,
    SourceBackendType,
)

BACKEND_TYPE_POSTGRES = SourceBackendType.declare("POSTGRES")
CONNECTION_TYPE_POSTGRES = ConnectionType.declare("postgres")
SOURCE_TYPE_PG_TABLE = CreateDSFrom.declare("PG_TABLE")
SOURCE_TYPE_PG_SUBSELECT = CreateDSFrom.declare("PG_SUBSELECT")
