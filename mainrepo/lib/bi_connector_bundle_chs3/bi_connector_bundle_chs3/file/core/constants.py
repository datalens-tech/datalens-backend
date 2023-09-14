from bi_constants.enums import (
    ConnectionType,
    CreateDSFrom,
)

CONNECTION_TYPE_FILE = ConnectionType.declare("file")
SOURCE_TYPE_FILE_S3_TABLE = CreateDSFrom.declare("FILE_S3_TABLE")
