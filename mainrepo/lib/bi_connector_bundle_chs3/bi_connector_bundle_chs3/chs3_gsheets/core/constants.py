from bi_constants.enums import ConnectionType, CreateDSFrom, NotificationType


CONNECTION_TYPE_GSHEETS_V2 = ConnectionType.gsheets_v2  # TODO: declaration
SOURCE_TYPE_GSHEETS_V2 = CreateDSFrom.GSHEETS_V2  # TODO: declaration

NOTIF_TYPE_GSHEETS_V2_STALE_DATA = NotificationType.declare('stale_data')
NOTIF_TYPE_GSHEETS_V2_DATA_UPDATE_FAILURE = NotificationType.declare('data_update_failure')
