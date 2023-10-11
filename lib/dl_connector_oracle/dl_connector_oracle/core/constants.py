from enum import (
    Enum,
    unique,
)

from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
    SourceBackendType,
)


BACKEND_TYPE_ORACLE = SourceBackendType.declare("ORACLE")
CONNECTION_TYPE_ORACLE = ConnectionType.declare("oracle")
SOURCE_TYPE_ORACLE_TABLE = DataSourceType.declare("ORACLE_TABLE")
SOURCE_TYPE_ORACLE_SUBSELECT = DataSourceType.declare("ORACLE_SUBSELECT")


@unique
class OracleDbNameType(Enum):
    sid = "sid"
    service_name = "service_name"
