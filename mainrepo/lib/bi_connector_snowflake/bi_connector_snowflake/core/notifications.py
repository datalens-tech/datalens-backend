from bi_constants.enums import NotificationLevel

from bi_api_commons.reporting import ReportingRegistry

from bi_core.reporting.notifications import get_notification_record, BaseNotification
from bi_core.us_connection_base import ConnectionBase

from bi_connector_snowflake.auth import SFAuthProvider
from bi_connector_snowflake.core.us_connection import ConnectionSQLSnowFlake
from bi_connector_snowflake.core.constants import NOTIF_TYPE_SF_REFRESH_TOKEN_SOON_TO_EXPIRE


class SnowflakeRefreshTokenSoonToExpire(BaseNotification):
    type = NOTIF_TYPE_SF_REFRESH_TOKEN_SOON_TO_EXPIRE
    _title = 'Snowflake connection token is going to expire soon'
    _message = '''Snowflake connection token is going to expire soon. \n\n
Please go to the connection details page and obtain a new refresh token. \n\n'''
    _level = NotificationLevel.warning


def check_for_refresh_token_expire(reporting_registry: ReportingRegistry, conn: ConnectionBase) -> None:
    assert isinstance(conn, ConnectionSQLSnowFlake)
    sf_auth_provider = SFAuthProvider.from_dto(conn.get_conn_dto())
    if sf_auth_provider.should_notify_refresh_token_to_expire_soon():
        reporting_registry.save_reporting_record(
            get_notification_record(NOTIF_TYPE_SF_REFRESH_TOKEN_SOON_TO_EXPIRE)
        )
