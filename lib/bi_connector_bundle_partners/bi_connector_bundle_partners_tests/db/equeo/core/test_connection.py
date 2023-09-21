from bi_connector_bundle_partners.equeo.core.constants import SOURCE_TYPE_EQUEO_CH_TABLE
from bi_connector_bundle_partners.equeo.core.settings import EqueoConnectorSettings
from bi_connector_bundle_partners.equeo.core.us_connection import EqueoCHConnection
from bi_connector_bundle_partners_tests.db.base.core.connection import PartnersConnectionTestClass
import bi_connector_bundle_partners_tests.db.config as test_config
from bi_connector_bundle_partners_tests.db.equeo.core.base import BaseEqueoTestClass


class TestEqueoConnection(BaseEqueoTestClass, PartnersConnectionTestClass[EqueoCHConnection]):
    source_type = SOURCE_TYPE_EQUEO_CH_TABLE
    sr_connection_settings = EqueoConnectorSettings(**test_config.SR_CONNECTION_SETTINGS_PARAMS)
