from dynamic_enum import (
    AutoEnumValue,
    DynamicEnum,
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


class TrinoAuthType(DynamicEnum):
    none = AutoEnumValue()
    password = AutoEnumValue()
    oauth2 = AutoEnumValue()
    kerberos = AutoEnumValue()
    certificate = AutoEnumValue()
    jwt = AutoEnumValue()
    header = AutoEnumValue()


ADAPTER_SOURCE_NAME = "datalens"
