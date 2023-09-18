from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase
from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS

from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.i18n.localizer import (
    CONFIGS as BI_CONNECTOR_BUNDLE_CH_FROZEN_CONFIGS,
)
from bi_connector_bundle_ch_frozen.ch_frozen_bumpy_roads.bi.connection_form.form_config import (
    CHFrozenBumpyRoadsFormFactory,
)
from bi_connector_bundle_ch_frozen.ch_frozen_covid.bi.connection_form.form_config import CHFrozenCovidFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_demo.bi.connection_form.form_config import CHFrozenDemoFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_dtp.bi.connection_form.form_config import CHFrozenDTPFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_gkh.bi.connection_form.form_config import CHFrozenGKHFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_samples.bi.connection_form.form_config import CHFrozenSamplesFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_transparency.bi.connection_form.form_config import (
    CHFrozenTransparencyFormFactory,
)
from bi_connector_bundle_ch_frozen.ch_frozen_weather.bi.connection_form.form_config import CHFrozenWeatherFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_horeca.bi.connection_form.form_config import CHFrozenHorecaFormFactory


class TestCHFrozenBumpyRoadsConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = CHFrozenBumpyRoadsFormFactory
    TRANSLATION_CONFIGS = BI_CONNECTOR_BUNDLE_CH_FROZEN_CONFIGS + BI_API_CONNECTOR_CONFIGS


class TestCHFrozenCovidConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = CHFrozenCovidFormFactory
    TRANSLATION_CONFIGS = BI_CONNECTOR_BUNDLE_CH_FROZEN_CONFIGS + BI_API_CONNECTOR_CONFIGS


class TestCHFrozenDemoConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = CHFrozenDemoFormFactory
    TRANSLATION_CONFIGS = BI_CONNECTOR_BUNDLE_CH_FROZEN_CONFIGS + BI_API_CONNECTOR_CONFIGS


class TestCHFrozenDTPConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = CHFrozenDTPFormFactory
    TRANSLATION_CONFIGS = BI_CONNECTOR_BUNDLE_CH_FROZEN_CONFIGS + BI_API_CONNECTOR_CONFIGS


class TestCHFrozenGKHConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = CHFrozenGKHFormFactory
    TRANSLATION_CONFIGS = BI_CONNECTOR_BUNDLE_CH_FROZEN_CONFIGS + BI_API_CONNECTOR_CONFIGS


class TestCHFrozenSamplesConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = CHFrozenSamplesFormFactory
    TRANSLATION_CONFIGS = BI_CONNECTOR_BUNDLE_CH_FROZEN_CONFIGS + BI_API_CONNECTOR_CONFIGS


class TestCHFrozenTransparencyConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = CHFrozenTransparencyFormFactory
    TRANSLATION_CONFIGS = BI_CONNECTOR_BUNDLE_CH_FROZEN_CONFIGS + BI_API_CONNECTOR_CONFIGS


class TestCHFrozenWeatherConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = CHFrozenWeatherFormFactory
    TRANSLATION_CONFIGS = BI_CONNECTOR_BUNDLE_CH_FROZEN_CONFIGS + BI_API_CONNECTOR_CONFIGS


class TestCHFrozenHorecaConnectionForm(ConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = CHFrozenHorecaFormFactory
    TRANSLATION_CONFIGS = BI_CONNECTOR_BUNDLE_CH_FROZEN_CONFIGS + BI_API_CONNECTOR_CONFIGS
