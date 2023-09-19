from dl_constants.enums import (
    ConnectionType,
    CreateDSFrom,
    SourceBackendType,
)


BACKEND_TYPE_BIGQUERY = SourceBackendType.declare("BIGQUERY")

CONNECTION_TYPE_BIGQUERY = ConnectionType.declare("bigquery")

SOURCE_TYPE_BIGQUERY_TABLE = CreateDSFrom.declare("BIGQUERY_TABLE")
SOURCE_TYPE_BIGQUERY_SUBSELECT = CreateDSFrom.declare("BIGQUERY_SUBSELECT")
