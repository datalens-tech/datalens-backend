from bi_constants.enums import NotificationLevel
from bi_core.reporting.notifications import BaseNotification

from bi_connector_snowflake.core.constants import NOTIF_TYPE_SF_REFRESH_TOKEN_SOON_TO_EXPIRE


class SnowflakeRefreshTokenSoonToExpire(BaseNotification):
    type = NOTIF_TYPE_SF_REFRESH_TOKEN_SOON_TO_EXPIRE
    _title = "Snowflake connection token is going to expire soon"
    _message = """Snowflake connection token is going to expire soon. \n\n
Please go to the connection details page and obtain a new refresh token. \n\n"""
    _level = NotificationLevel.warning
