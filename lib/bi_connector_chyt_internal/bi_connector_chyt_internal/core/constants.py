from dl_connector_chyt.core.constants import BACKEND_TYPE_CHYT as BASE_BACKEND_TYPE_CHYT
from dl_constants.enums import (
    ConnectionType,
    DataSourceCreatedVia,
    DataSourceType,
    NotificationType,
)


BACKEND_TYPE_CHYT = BASE_BACKEND_TYPE_CHYT

# Token connection
CONNECTION_TYPE_CH_OVER_YT = ConnectionType.declare("ch_over_yt")

SOURCE_TYPE_CHYT_TABLE = DataSourceType.declare("CHYT_TABLE")
SOURCE_TYPE_CHYT_SUBSELECT = DataSourceType.declare("CHYT_SUBSELECT")
SOURCE_TYPE_CHYT_TABLE_LIST = DataSourceType.declare("CHYT_TABLE_LIST")
SOURCE_TYPE_CHYT_TABLE_RANGE = DataSourceType.declare("CHYT_TABLE_RANGE")

# User auth connection
CONNECTION_TYPE_CH_OVER_YT_USER_AUTH = ConnectionType.declare("ch_over_yt_user_auth")

SOURCE_TYPE_CHYT_USER_AUTH_TABLE = DataSourceType.declare("CHYT_USER_AUTH_TABLE")
SOURCE_TYPE_CHYT_USER_AUTH_SUBSELECT = DataSourceType.declare("CHYT_USER_AUTH_SUBSELECT")
SOURCE_TYPE_CHYT_USER_AUTH_TABLE_LIST = DataSourceType.declare("CHYT_USER_AUTH_TABLE_LIST")
SOURCE_TYPE_CHYT_USER_AUTH_TABLE_RANGE = DataSourceType.declare("CHYT_USER_AUTH_TABLE_RANGE")

NOTIF_TYPE_CHYT_USING_PUBLIC_CLIQUE = NotificationType.declare("using_public_clickhouse_clique")

DATA_SOURCE_CREATE_VIA_YT_TO_DL = DataSourceCreatedVia.declare("yt_to_dl")
