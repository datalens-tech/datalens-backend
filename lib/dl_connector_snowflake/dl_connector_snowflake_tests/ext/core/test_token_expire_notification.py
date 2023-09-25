import pytest as pytest

from dl_connector_snowflake_tests.ext.core.base import BaseSnowFlakeTestClass


class TestSnowflakeConnectionNotification(BaseSnowFlakeTestClass):
    @pytest.mark.asyncio
    async def test_refresh_token_notification_soon_to_expire(self, saved_connection_with_refresh_token_soon_to_expire):
        notifications = saved_connection_with_refresh_token_soon_to_expire.check_for_notifications()
        record = notifications[0]
        assert record.locator == "snowflake_refresh_token_soon_to_expire"
