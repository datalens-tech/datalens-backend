from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase

from bi_connector_bundle_ch_frozen.ch_frozen_base.api.i18n.localizer import (
    CONFIGS as BI_CONNECTOR_BUNDLE_CH_FROZEN_CONFIGS,
)
from bi_connector_bundle_ch_frozen.ch_frozen_bumpy_roads.api.connection_form.form_config import (
    CHFrozenBumpyRoadsFormFactory,
)
from bi_connector_bundle_ch_frozen.ch_frozen_covid.api.connection_form.form_config import CHFrozenCovidFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_demo.api.connection_form.form_config import CHFrozenDemoFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_dtp.api.connection_form.form_config import CHFrozenDTPFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_gkh.api.connection_form.form_config import CHFrozenGKHFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_horeca.api.connection_form.form_config import CHFrozenHorecaFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_samples.api.connection_form.form_config import CHFrozenSamplesFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_transparency.api.connection_form.form_config import (
    CHFrozenTransparencyFormFactory,
)
from bi_connector_bundle_ch_frozen.ch_frozen_weather.api.connection_form.form_config import CHFrozenWeatherFormFactory


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
