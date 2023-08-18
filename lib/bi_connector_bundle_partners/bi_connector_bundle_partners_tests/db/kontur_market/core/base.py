from __future__ import annotations

import pytest

from bi_constants.enums import ConnectionType

from bi_configs.connectors_settings import ConnectorsSettingsByType, KonturMarketConnectorSettings

from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_connector_bundle_partners.kontur_market.core.us_connection import KonturMarketCHConnection
from bi_connector_bundle_partners.kontur_market.core.testing.connection import make_saved_kontur_market_connection

from bi_connector_bundle_partners_tests.db.base.core.base import BasePartnersClass

import bi_connector_bundle_partners_tests.db.config as test_config


class BaseKonturMarketTestClass(BasePartnersClass[KonturMarketCHConnection]):
    conn_type = ConnectionType.kontur_market
    connection_settings = ConnectorsSettingsByType(
        KONTUR_MARKET=KonturMarketConnectorSettings(**test_config.SR_CONNECTION_SETTINGS_PARAMS)
    )

    @pytest.fixture(scope='function')
    def saved_connection(
            self, sync_us_manager: SyncUSManager, connection_creation_params: dict
    ) -> KonturMarketCHConnection:
        return make_saved_kontur_market_connection(
            sync_usm=sync_us_manager,
            **connection_creation_params
        )
