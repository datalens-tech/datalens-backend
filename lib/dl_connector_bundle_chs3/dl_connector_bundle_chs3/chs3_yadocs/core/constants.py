from dl_constants.enums import (
    ConnectionType,
    DataSourceType,
    NotificationType,
)
from dl_connector_bundle_chs3.constants import NOTIF_TYPE_STALE_DATA, NOTIF_TYPE_DATA_UPDATE_FAILURE

CONNECTION_TYPE_YADOCS = ConnectionType.declare("yadocs")
SOURCE_TYPE_YADOCS = DataSourceType.declare("YADOCS")

NOTIF_TYPE_YADOCS_STALE_DATA = NOTIF_TYPE_STALE_DATA
NOTIF_TYPE_YADOCS_DATA_UPDATE_FAILURE = NOTIF_TYPE_DATA_UPDATE_FAILURE
