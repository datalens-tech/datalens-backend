from __future__ import annotations

from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.settings import MarketCouriersConnectorSettings
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.core.us_connection import (
    ConnectionClickhouseMarketCouriers,
)

from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.config import SR_CONNECTION_SETTINGS_PARAMS
from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.connection import (
    BaseClickhouseFilteredSubselectByPuidConnectionTestClass,
    ClickhouseFilteredSubselectByPuidConnectionTestWithWrongAuth,
)

from bi_connector_bundle_ch_filtered_ya_cloud_tests.ext.market_couriers.core.base import (
    BaseMarketCouriersTestClass, MarketCouriersTestClassWithWrongAuth
)


class TestMarketCouriersConnection(
        BaseMarketCouriersTestClass,
        BaseClickhouseFilteredSubselectByPuidConnectionTestClass[ConnectionClickhouseMarketCouriers],
):
    sr_connection_settings = MarketCouriersConnectorSettings(**SR_CONNECTION_SETTINGS_PARAMS)


class TestMarketCouriersConnectionWithWrongAuth(
        MarketCouriersTestClassWithWrongAuth,
        ClickhouseFilteredSubselectByPuidConnectionTestWithWrongAuth
):
    pass
