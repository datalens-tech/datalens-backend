from __future__ import annotations

import pytest

from dl_core.us_manager.us_manager_sync import SyncUSManager

from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.constants import CONNECTION_TYPE_MARKET_COURIERS
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.settings import MarketCouriersConnectorSettings
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.testing.connection import (
    make_saved_market_couriers_connection,
)
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.us_connection import (
    ConnectionClickhouseMarketCouriers,
)
from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.base import (
    BaseClickhouseFilteredSubselectByPuidTestClass,
    ClickhouseFilteredSubselectByPuidTestClassWithWrongAuth,
)
import bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.config as test_config


class BaseMarketCouriersTestClass(BaseClickhouseFilteredSubselectByPuidTestClass[ConnectionClickhouseMarketCouriers]):
    conn_type = CONNECTION_TYPE_MARKET_COURIERS
    connection_settings = MarketCouriersConnectorSettings(**test_config.SR_CONNECTION_SETTINGS_PARAMS)

    @pytest.fixture(scope="function")
    def saved_connection(
        self, sync_us_manager: SyncUSManager, connection_creation_params: dict
    ) -> ConnectionClickhouseMarketCouriers:
        conn = make_saved_market_couriers_connection(sync_usm=sync_us_manager, **connection_creation_params)
        return sync_us_manager.get_by_id(conn.uuid)  # to invoke a lifecycle manager


class MarketCouriersTestClassWithWrongAuth(
    BaseMarketCouriersTestClass,
    ClickhouseFilteredSubselectByPuidTestClassWithWrongAuth[ConnectionClickhouseMarketCouriers],
):
    pass
