from enum import (
    Enum,
    unique,
)

from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
    SourceBackendType,
)


BACKEND_TYPE_YDB = SourceBackendType.declare("YDB")

CONNECTION_TYPE_YDB = ConnectionType.declare("ydb")

SOURCE_TYPE_YDB_TABLE = DataSourceType.declare("YDB_TABLE")
SOURCE_TYPE_YDB_SUBSELECT = DataSourceType.declare("YDB_SUBSELECT")


@unique
class YDBAuthTypeMode(Enum):
    anonymous = "anonymous"
    password = "password"
    oauth = "oauth"
