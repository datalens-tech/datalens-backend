from dl_constants.enums import NotificationType

from dl_connector_clickhouse.core.clickhouse_base.constants import BACKEND_TYPE_CLICKHOUSE


BACKEND_TYPE_CHS3 = BACKEND_TYPE_CLICKHOUSE

NOTIF_TYPE_STALE_DATA = NotificationType.declare("stale_data")
NOTIF_TYPE_DATA_UPDATE_FAILURE = NotificationType.declare("data_update_failure")
