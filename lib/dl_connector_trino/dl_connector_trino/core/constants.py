from enum import (
    Enum,
    unique,
)

from dl_constants.enums import (  # NotificationType,
    ConnectionType,
    DataSourceType,
    SourceBackendType,
)


BACKEND_TYPE_TRINO = SourceBackendType.declare("TRINO")
CONNECTION_TYPE_TRINO = ConnectionType.declare("trino")  # FIXME: Move the declaration here
SOURCE_TYPE_TRINO_TABLE = DataSourceType.declare("TRINO_TABLE")
SOURCE_TYPE_TRINO_SUBSELECT = DataSourceType.declare("TRINO_SUBSELECT")


@unique
class TrinoAuthType(str, Enum):
    NONE = "none"
    PASSWORD = "password"
    OAUTH2 = "oauth2"
    KERBEROS = "kerberos"
    CERTIFICATE = "certificate"
    JWT = "jwt"
    HEADER = "header"

    def __str__(self) -> str:
        return self.value


ADAPTER_SOURCE_NAME = "datalens"
