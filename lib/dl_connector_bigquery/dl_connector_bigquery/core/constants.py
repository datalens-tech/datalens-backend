from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
    SourceBackendType,
)


BACKEND_TYPE_BIGQUERY = SourceBackendType.declare("BIGQUERY")

CONNECTION_TYPE_BIGQUERY = ConnectionType.declare("bigquery")

SOURCE_TYPE_BIGQUERY_TABLE = DataSourceType.declare("BIGQUERY_TABLE")
SOURCE_TYPE_BIGQUERY_SUBSELECT = DataSourceType.declare("BIGQUERY_SUBSELECT")
