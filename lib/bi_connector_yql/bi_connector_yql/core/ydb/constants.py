from dl_constants.enums import (
    ConnectionType,
    CreateDSFrom,
    SourceBackendType,
)


BACKEND_TYPE_YDB = SourceBackendType.declare("YDB")

CONNECTION_TYPE_YDB = ConnectionType.declare("ydb")

SOURCE_TYPE_YDB_TABLE = CreateDSFrom.declare("YDB_TABLE")
SOURCE_TYPE_YDB_SUBSELECT = CreateDSFrom.declare("YDB_SUBSELECT")
