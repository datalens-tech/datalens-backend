from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase

from bi_connector_bitrix_gds.api.connection_form.form_config import BitrixGDSConnectionFormFactory
from bi_connector_bitrix_gds.api.i18n.localizer import CONFIGS as BI_CONNECTOR_BITRIX_GDS_CONFIGS


class TestBitrixGDSConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = BitrixGDSConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_BITRIX_GDS_CONFIGS
