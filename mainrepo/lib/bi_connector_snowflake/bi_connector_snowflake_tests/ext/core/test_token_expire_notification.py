import pytest as pytest

from bi_api_commons.base_models import RequestContextInfo
from bi_api_commons.reporting import DefaultReportingRegistry
from bi_connector_snowflake.core.notifications import check_for_refresh_token_expire

from bi_connector_snowflake_tests.ext.core.base import BaseSnowFlakeTestClass


class TestSnowflakeConnectionNotification(BaseSnowFlakeTestClass):

    @pytest.mark.asyncio
    async def test_refresh_token_notification_soon_to_expire(self, saved_connection_with_refresh_token_soon_to_expire):
        rci = RequestContextInfo()
        rr = DefaultReportingRegistry(rci=rci)

        check_for_refresh_token_expire(rr, saved_connection_with_refresh_token_soon_to_expire)
        record = rr._reporting_records[0]
        assert record.locator == "snowflake_refresh_token_soon_to_expire"
