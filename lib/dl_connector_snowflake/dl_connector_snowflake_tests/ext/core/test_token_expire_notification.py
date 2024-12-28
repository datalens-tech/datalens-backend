import pytest as pytest

from dl_connector_snowflake_tests.ext.core.base import SnowFlakeTestClassWithRefreshTokenSoonToExpire


class TestSnowflakeConnectionNotification(SnowFlakeTestClassWithRefreshTokenSoonToExpire):
    @pytest.mark.asyncio
    async def test_refresh_token_notification_soon_to_expire(self, saved_connection):
        notifications = saved_connection.check_for_notifications()
        record = notifications[0]
        assert record.locator == "snowflake_refresh_token_soon_to_expire"
