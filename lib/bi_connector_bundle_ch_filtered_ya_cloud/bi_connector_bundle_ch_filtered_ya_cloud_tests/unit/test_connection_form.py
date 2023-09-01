from bi_api_lib_testing.connection_form_base import ConnectionFormTestBase
from bi_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS

from bi_connector_bundle_ch_filtered.base.bi.i18n.localizer import CONFIGS as BI_CONNECTOR_BUNDLE_CH_FILTERED_CONFIGS
from bi_connector_bundle_ch_filtered_ya_cloud.base.bi.i18n.localizer import (
    CONFIGS as BI_CONNECTOR_BUNDLE_CH_FILTERED_YA_CLOUD_CONFIGS,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_geo_filtered.bi.connection_form.form_config import (
    CHGeoFilteredFormFactory,
)
from bi_connector_bundle_ch_filtered_ya_cloud.ch_ya_music_podcast_stats.bi.connection_form.form_config import (
    CHYaMusicPodcastStatsConnectionFormFactory,
)
from bi_connector_bundle_ch_filtered_ya_cloud.market_couriers.bi.connection_form.form_config import (
    CHMarketCouriersConnectionFormFactory,
)
from bi_connector_bundle_ch_filtered_ya_cloud.schoolbook.bi.connection_form.form_config import (
    CHSchoolbookConnectionFormFactory,
)
from bi_connector_bundle_ch_filtered_ya_cloud.smb_heatmaps.bi.connection_form.form_config import (
    CHSMBHeatmapsConnectionFormFactory,
)


class TestCHGeoFilteredConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = CHGeoFilteredFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_BUNDLE_CH_FILTERED_YA_CLOUD_CONFIGS


CONFIGS = (
    BI_API_CONNECTOR_CONFIGS +
    BI_CONNECTOR_BUNDLE_CH_FILTERED_CONFIGS +
    BI_CONNECTOR_BUNDLE_CH_FILTERED_YA_CLOUD_CONFIGS
)


class TestCHYaMusicPodcastStatsTestConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = CHYaMusicPodcastStatsConnectionFormFactory
    TRANSLATION_CONFIGS = CONFIGS


class TestCHMarketCouriersConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = CHMarketCouriersConnectionFormFactory
    TRANSLATION_CONFIGS = CONFIGS


class TestCHSchoolbookConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = CHSchoolbookConnectionFormFactory
    TRANSLATION_CONFIGS = CONFIGS


class TestCHSMBHeatmapsConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = CHSMBHeatmapsConnectionFormFactory
    TRANSLATION_CONFIGS = CONFIGS
