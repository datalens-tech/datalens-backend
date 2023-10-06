from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
    NotificationType,
)


CONNECTION_TYPE_GSHEETS_V2 = ConnectionType.declare("gsheets_v2")
SOURCE_TYPE_GSHEETS_V2 = DataSourceType.declare("GSHEETS_V2")

NOTIF_TYPE_GSHEETS_V2_STALE_DATA = NotificationType.declare("stale_data")
NOTIF_TYPE_GSHEETS_V2_DATA_UPDATE_FAILURE = NotificationType.declare("data_update_failure")
