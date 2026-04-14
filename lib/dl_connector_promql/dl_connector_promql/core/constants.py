from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
    SourceBackendType,
)
from dl_dynamic_enum import (
    AutoEnumValue,
    DynamicEnum,
)


BACKEND_TYPE_PROMQL = SourceBackendType.declare("PROMQL")
CONNECTION_TYPE_PROMQL = ConnectionType.declare("promql")
SOURCE_TYPE_PROMQL = DataSourceType.declare("PROMQL")


class PromQLAuthType(DynamicEnum):
    password = AutoEnumValue()
    header = AutoEnumValue()
