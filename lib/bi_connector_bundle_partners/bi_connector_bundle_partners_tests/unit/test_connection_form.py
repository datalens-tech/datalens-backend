from bi_api_lib_testing.connection_form_base import ConnectionFormTestBase
from bi_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS

from bi_connector_bundle_partners.base.bi.i18n.localizer import CONFIGS as BI_CONNECTOR_BUNDLE_PARTNERS_CONFIGS
from bi_connector_bundle_partners.equeo.bi.connection_form.form_config import EqueoConnectionFormFactory
from bi_connector_bundle_partners.kontur_market.bi.connection_form.form_config import KonturMarketConnectionFormFactory
from bi_connector_bundle_partners.moysklad.bi.connection_form.form_config import MoySkladConnectionFormFactory


class TestEqueoConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = EqueoConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_BUNDLE_PARTNERS_CONFIGS


class TestMoySkladConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = MoySkladConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_BUNDLE_PARTNERS_CONFIGS


class TestKonturMarketConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = KonturMarketConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_BUNDLE_PARTNERS_CONFIGS
