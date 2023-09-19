from dl_constants.enums import (
    ConnectionType,
    CreateDSFrom,
    SourceBackendType,
)


BACKEND_TYPE_GREENPLUM = SourceBackendType.declare("GREENPLUM")
CONNECTION_TYPE_GREENPLUM = ConnectionType.declare("greenplum")
SOURCE_TYPE_GP_TABLE = CreateDSFrom.declare("GP_TABLE")
SOURCE_TYPE_GP_SUBSELECT = CreateDSFrom.declare("GP_SUBSELECT")
