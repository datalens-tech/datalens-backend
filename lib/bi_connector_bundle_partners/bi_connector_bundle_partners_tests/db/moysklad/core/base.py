from __future__ import annotations

import pytest

from bi_constants.enums import ConnectionType

from bi_configs.connectors_settings import MoySkladConnectorSettings

from bi_core.us_manager.us_manager_sync import SyncUSManager
from bi_connector_bundle_partners.moysklad.core.us_connection import MoySkladCHConnection
from bi_connector_bundle_partners.moysklad.core.testing.connection import make_saved_moysklad_connection

from bi_connector_bundle_partners_tests.db.base.core.base import BasePartnersClass

import bi_connector_bundle_partners_tests.db.config as test_config


class BaseMoySkladTestClass(BasePartnersClass[MoySkladCHConnection]):
    conn_type = ConnectionType.moysklad
    connection_settings = MoySkladConnectorSettings(**test_config.SR_CONNECTION_SETTINGS_PARAMS)

    @pytest.fixture(scope='function')
    def saved_connection(
            self, sync_us_manager: SyncUSManager, connection_creation_params: dict
    ) -> MoySkladCHConnection:
        return make_saved_moysklad_connection(
            sync_usm=sync_us_manager,
            **connection_creation_params
        )
