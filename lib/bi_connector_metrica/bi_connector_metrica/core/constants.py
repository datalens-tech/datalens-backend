from dl_constants.enums import (
    ConnectionType,
    CreateDSFrom,
    SourceBackendType,
)


BACKEND_TYPE_METRICA_API = SourceBackendType.declare("METRICA_API")
CONNECTION_TYPE_METRICA_API = ConnectionType.declare("metrika_api")  # Note the K in the value
SOURCE_TYPE_METRICA_API = CreateDSFrom.declare("METRIKA_API")  # Note the K in the value

BACKEND_TYPE_APPMETRICA_API = SourceBackendType.declare("APPMETRICA_API")
CONNECTION_TYPE_APPMETRICA_API = ConnectionType.declare("appmetrica_api")
SOURCE_TYPE_APPMETRICA_API = CreateDSFrom.declare("APPMETRICA_API")
