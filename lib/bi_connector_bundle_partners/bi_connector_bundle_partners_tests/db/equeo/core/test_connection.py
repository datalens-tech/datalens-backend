from bi_configs.connectors_settings import EqueoConnectorSettings

from bi_connector_bundle_partners.equeo.core.us_connection import EqueoCHConnection
from bi_connector_bundle_partners_tests.db.base.core.connection import PartnersConnectionTestClass
from bi_connector_bundle_partners_tests.db.equeo.core.base import BaseEqueoTestClass

import bi_connector_bundle_partners_tests.db.config as test_config


class TestEqueoConnection(BaseEqueoTestClass, PartnersConnectionTestClass[EqueoCHConnection]):
    sr_connection_settings = EqueoConnectorSettings(**test_config.SR_CONNECTION_SETTINGS_PARAMS)
