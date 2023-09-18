from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase
from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS

from bi_connector_bundle_ch_filtered.base.bi.i18n.localizer import CONFIGS as BI_CONNECTOR_BUNDLE_CH_FILTERED_CONFIGS
from bi_connector_bundle_ch_filtered.ch_billing_analytics.bi.connection_form.form_config import (
    CHBillingAnalyticsConnectionFormFactory,
)


class TestCHBillingAnalyticsConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = CHBillingAnalyticsConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_BUNDLE_CH_FILTERED_CONFIGS
