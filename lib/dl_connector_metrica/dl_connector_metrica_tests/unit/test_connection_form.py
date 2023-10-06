import pytest

from dl_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS
from dl_api_lib_testing.connection_form_base import ConnectionFormTestBase

from dl_connector_metrica.api.connection_form.form_config import (
    AppMetricaAPIConnectionFormFactory,
    MetricaAPIConnectionFormFactory,
)
from dl_connector_metrica.api.i18n.localizer import CONFIGS as BI_CONNECTOR_METRICA_CONFIGS
from dl_connector_metrica.core.settings import (
    AppmetricaConnectorSettings,
    MetricaConnectorSettings,
)


class MetricaLikeConnectionFormTestBase(ConnectionFormTestBase):
    @pytest.fixture(
        params=(True, False),
        ids=("auto_dash_True", "auto_dash_False"),
    )
    def allow_auto_dash_creation(self, request) -> bool:
        return request.param

    @pytest.fixture(
        params=(True, False),
        ids=("manual_counter_True", "manual_counter_False"),
    )
    def allow_counter_manual_input(self, request) -> bool:
        return request.param


class TestMetricaAPIConnectionForm(MetricaLikeConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = MetricaAPIConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_METRICA_CONFIGS

    @pytest.fixture
    def connectors_settings(  # noqa
        self,
        allow_auto_dash_creation,
        allow_counter_manual_input,
    ) -> MetricaConnectorSettings:
        return MetricaConnectorSettings(
            COUNTER_ALLOW_MANUAL_INPUT=allow_counter_manual_input,
            ALLOW_AUTO_DASH_CREATION=allow_auto_dash_creation,
        )


class TestAppMetricaAPIConnectionForm(MetricaLikeConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = AppMetricaAPIConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_CONNECTOR_METRICA_CONFIGS

    @pytest.fixture
    def connectors_settings(  # noqa
        self,
        allow_auto_dash_creation,
        allow_counter_manual_input,
    ) -> AppmetricaConnectorSettings:
        return AppmetricaConnectorSettings(
            COUNTER_ALLOW_MANUAL_INPUT=allow_counter_manual_input,
            ALLOW_AUTO_DASH_CREATION=allow_auto_dash_creation,
        )
