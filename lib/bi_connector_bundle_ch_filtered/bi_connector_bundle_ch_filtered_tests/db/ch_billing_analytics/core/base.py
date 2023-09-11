from __future__ import annotations

import asyncio
from typing import Generator

import pytest

from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.constants import (
    CONNECTION_TYPE_CH_BILLING_ANALYTICS,
)
from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.us_connection import BillingAnalyticsCHConnection
from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.lifecycle import (
    BillingAnalyticsCHConnectionLifecycleManager,
)
from bi_connector_bundle_ch_filtered.ch_billing_analytics.core.testing.connection import (
    make_saved_ch_billing_analytics_connection,
)
from bi_core_testing.testcases.connection import BaseConnectionTestClass

import bi_connector_bundle_ch_filtered_tests.db.config as common_test_config
import bi_connector_bundle_ch_filtered_tests.db.ch_billing_analytics.config as test_config


class BaseCHBillingAnalyticsTestClass(BaseConnectionTestClass[BillingAnalyticsCHConnection]):
    conn_type = CONNECTION_TYPE_CH_BILLING_ANALYTICS
    core_test_config = common_test_config.CORE_TEST_CONFIG
    connection_settings = test_config.SR_CONNECTION_SETTINGS
    inst_specific_sr_factory = common_test_config.YC_SR_FACTORY

    @pytest.fixture(autouse=True)
    # FIXME: This fixture is a temporary solution for failing core tests when they are run together with api tests
    def loop(self, event_loop: asyncio.AbstractEventLoop) -> Generator[asyncio.AbstractEventLoop, None, None]:
        asyncio.set_event_loop(event_loop)
        yield event_loop
        # Attempt to cover an old version of pytest-asyncio:
        # https://github.com/pytest-dev/pytest-asyncio/commit/51d986cec83fdbc14fa08015424c79397afc7ad9
        asyncio.set_event_loop_policy(None)

    @pytest.fixture(scope='class')
    def db_url(self) -> str:
        return common_test_config.DB_CORE_URL

    @pytest.fixture(scope='function')
    def connection_creation_params(self) -> dict:
        return dict(
            endpoint=common_test_config.CoreConnectionSettings.ENDPOINT,
            cluster_name=common_test_config.CoreConnectionSettings.CLUSTER_NAME,
            max_execution_time=common_test_config.CoreConnectionSettings.MAX_EXECUTION_TIME,
            **(dict(raw_sql_level=self.raw_sql_level) if self.raw_sql_level is not None else {}),
        )

    @pytest.fixture(scope='function')
    def saved_connection(
            self, sync_us_manager: SyncUSManager, connection_creation_params: dict, monkeypatch
    ) -> BillingAnalyticsCHConnection:
        async def _post_init_hook(self: BillingAnalyticsCHConnectionLifecycleManager) -> None:
            self.entry.billing_accounts = ['some_ba_id_1', 'some_ba_id_2']

        monkeypatch.setattr(
            BillingAnalyticsCHConnectionLifecycleManager, 'post_init_async_hook', _post_init_hook
        )
        conn = make_saved_ch_billing_analytics_connection(
            sync_usm=sync_us_manager,
            **connection_creation_params
        )
        return sync_us_manager.get_by_id(conn.uuid)  # to invoke a lifecycle manager
