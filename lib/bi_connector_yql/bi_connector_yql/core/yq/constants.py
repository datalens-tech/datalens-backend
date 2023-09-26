from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
    SourceBackendType,
)


BACKEND_TYPE_YQ = SourceBackendType.declare("YQ")

CONNECTION_TYPE_YQ = ConnectionType.declare("yq")

SOURCE_TYPE_YQ_TABLE = DataSourceType.declare("YQ_TABLE")
SOURCE_TYPE_YQ_SUBSELECT = DataSourceType.declare("YQ_SUBSELECT")
