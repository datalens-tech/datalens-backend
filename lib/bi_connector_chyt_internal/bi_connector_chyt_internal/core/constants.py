from bi_constants.enums import ConnectionType, CreateDSFrom

from bi_connector_chyt.core.constants import BACKEND_TYPE_CHYT as BASE_BACKEND_TYPE_CHYT


BACKEND_TYPE_CHYT = BASE_BACKEND_TYPE_CHYT

# Token connection
CONNECTION_TYPE_CH_OVER_YT = ConnectionType.ch_over_yt  # FIXME: declaration

SOURCE_TYPE_CHYT_TABLE = CreateDSFrom.CHYT_TABLE  # FIXME: declaration
SOURCE_TYPE_CHYT_SUBSELECT = CreateDSFrom.CHYT_SUBSELECT  # FIXME: declaration
SOURCE_TYPE_CHYT_TABLE_LIST = CreateDSFrom.CHYT_TABLE_LIST  # FIXME: declaration
SOURCE_TYPE_CHYT_TABLE_RANGE = CreateDSFrom.CHYT_TABLE_RANGE  # FIXME: declaration

# User auth connection
CONNECTION_TYPE_CH_OVER_YT_USER_AUTH = ConnectionType.ch_over_yt_user_auth  # FIXME: declaration

SOURCE_TYPE_CHYT_USER_AUTH_TABLE = CreateDSFrom.CHYT_USER_AUTH_TABLE  # FIXME: declaration
SOURCE_TYPE_CHYT_USER_AUTH_SUBSELECT = CreateDSFrom.CHYT_USER_AUTH_SUBSELECT  # FIXME: declaration
SOURCE_TYPE_CHYT_USER_AUTH_TABLE_LIST = CreateDSFrom.CHYT_USER_AUTH_TABLE_LIST  # FIXME: declaration
SOURCE_TYPE_CHYT_USER_AUTH_TABLE_RANGE = CreateDSFrom.CHYT_USER_AUTH_TABLE_RANGE  # FIXME: declaration
