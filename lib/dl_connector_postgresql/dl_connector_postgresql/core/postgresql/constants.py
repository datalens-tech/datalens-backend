from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
    SourceBackendType,
)


BACKEND_TYPE_POSTGRES = SourceBackendType.declare("POSTGRES")
CONNECTION_TYPE_POSTGRES = ConnectionType.declare("postgres")
SOURCE_TYPE_PG_TABLE = DataSourceType.declare("PG_TABLE")
SOURCE_TYPE_PG_SUBSELECT = DataSourceType.declare("PG_SUBSELECT")
