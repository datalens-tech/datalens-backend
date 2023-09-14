from bi_constants.enums import (
    ConnectionType,
    CreateDSFrom,
    SourceBackendType,
)

BACKEND_TYPE_CLICKHOUSE = SourceBackendType.CLICKHOUSE  # TODO: Move declaration here
CONNECTION_TYPE_CLICKHOUSE = ConnectionType.clickhouse  # TODO: Move declaration here
SOURCE_TYPE_CH_TABLE = CreateDSFrom.CH_TABLE  # TODO: Move declaration here
SOURCE_TYPE_CH_SUBSELECT = CreateDSFrom.declare("CH_SUBSELECT")
