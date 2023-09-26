from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
    SourceBackendType,
)


BACKEND_TYPE_GREENPLUM = SourceBackendType.declare("GREENPLUM")
CONNECTION_TYPE_GREENPLUM = ConnectionType.declare("greenplum")
SOURCE_TYPE_GP_TABLE = DataSourceType.declare("GP_TABLE")
SOURCE_TYPE_GP_SUBSELECT = DataSourceType.declare("GP_SUBSELECT")
