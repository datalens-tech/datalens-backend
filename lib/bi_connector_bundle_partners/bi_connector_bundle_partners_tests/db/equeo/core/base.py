from __future__ import annotations

import pytest

from bi_core.us_manager.us_manager_sync import SyncUSManager

from bi_connector_bundle_partners.equeo.core.constants import CONNECTION_TYPE_EQUEO
from bi_connector_bundle_partners.equeo.core.settings import EqueoConnectorSettings
from bi_connector_bundle_partners.equeo.core.us_connection import EqueoCHConnection
from bi_connector_bundle_partners.equeo.core.testing.connection import make_saved_equeo_connection

from bi_connector_bundle_partners_tests.db.base.core.base import BasePartnersClass

import bi_connector_bundle_partners_tests.db.config as test_config


class BaseEqueoTestClass(BasePartnersClass[EqueoCHConnection]):
    conn_type = CONNECTION_TYPE_EQUEO
    connection_settings = EqueoConnectorSettings(**test_config.SR_CONNECTION_SETTINGS_PARAMS)

    @pytest.fixture(scope='function')
    def saved_connection(
            self, sync_us_manager: SyncUSManager, connection_creation_params: dict
    ) -> EqueoCHConnection:
        return make_saved_equeo_connection(
            sync_usm=sync_us_manager,
            **connection_creation_params
        )
