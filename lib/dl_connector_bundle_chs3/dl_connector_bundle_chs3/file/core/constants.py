from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
    SourceBackendType,
)


BACKEND_TYPE_FILE = SourceBackendType.declare("FILE")
CONNECTION_TYPE_FILE = ConnectionType.declare("file")
SOURCE_TYPE_FILE_S3_TABLE = DataSourceType.declare("FILE_S3_TABLE")
