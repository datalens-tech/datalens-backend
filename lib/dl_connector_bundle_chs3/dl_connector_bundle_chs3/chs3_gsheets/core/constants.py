from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
    NotificationType,
)

from dl_connector_bundle_chs3.constants import (
    NOTIF_TYPE_DATA_UPDATE_FAILURE,
    NOTIF_TYPE_STALE_DATA,
)


CONNECTION_TYPE_GSHEETS_V2 = ConnectionType.declare("gsheets_v2")
SOURCE_TYPE_GSHEETS_V2 = DataSourceType.declare("GSHEETS_V2")

NOTIF_TYPE_GSHEETS_V2_STALE_DATA = NOTIF_TYPE_STALE_DATA
NOTIF_TYPE_GSHEETS_V2_DATA_UPDATE_FAILURE = NOTIF_TYPE_DATA_UPDATE_FAILURE
