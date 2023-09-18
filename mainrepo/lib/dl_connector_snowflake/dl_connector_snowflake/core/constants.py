from dl_constants.enums import (
    ConnectionType,
    CreateDSFrom,
    NotificationType,
    SourceBackendType,
)

BACKEND_TYPE_SNOWFLAKE = SourceBackendType.declare("SNOWFLAKE")

CONNECTION_TYPE_SNOWFLAKE = ConnectionType.declare("snowflake")  # FIXME: Move the declaration here

SOURCE_TYPE_SNOWFLAKE_TABLE = CreateDSFrom.declare("SNOWFLAKE_TABLE")
SOURCE_TYPE_SNOWFLAKE_SUBSELECT = CreateDSFrom.declare("SNOWFLAKE_SUBSELECT")

NOTIF_TYPE_SF_REFRESH_TOKEN_SOON_TO_EXPIRE = NotificationType.declare("snowflake_refresh_token_soon_to_expire")

ACCOUNT_NAME_RE = r"^[a-zA-Z\d\-\_\.]+$"
