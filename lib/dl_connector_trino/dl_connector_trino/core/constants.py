from enum import (
    Enum,
    unique,
)

from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
    SourceBackendType,
)


BACKEND_TYPE_TRINO = SourceBackendType.declare("TRINO")
CONNECTION_TYPE_TRINO = ConnectionType.declare("trino")
SOURCE_TYPE_TRINO_TABLE = DataSourceType.declare("TRINO_TABLE")
SOURCE_TYPE_TRINO_SUBSELECT = DataSourceType.declare("TRINO_SUBSELECT")


@unique
class TrinoAuthType(str, Enum):
    none = "none"
    password = "password"
    oauth2 = "oauth2"
    kerberos = "kerberos"
    certificate = "certificate"
    jwt = "jwt"
    header = "header"

    def __str__(self) -> str:
        return self.value


ADAPTER_SOURCE_NAME = "datalens"
