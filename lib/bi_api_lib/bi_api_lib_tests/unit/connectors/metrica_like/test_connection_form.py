from typing import Optional

import pytest

from bi_configs.connectors_settings import (
    ConnectorsSettingsByType,
    MetricaConnectorSettings,
    AppmetricaConnectorSettings,
)

from bi_api_connector.form_config.testing.test_connection_form_base import ConnectionFormTestBase
from bi_api_connector.i18n.localizer import CONFIGS as BI_API_CONNECTOR_CONFIGS

from bi_api_lib.connectors.metrica.connection_form.form_config import (
    MetricaAPIConnectionFormFactory, AppMetricaAPIConnectionFormFactory,
)
from bi_api_lib.i18n.localizer import CONFIGS as BI_API_LIB_CONFIGS


class MetricaLikeConnectionFormTestBase(ConnectionFormTestBase):
    @pytest.fixture(
        params=(True, False),
        ids=('auto_dash_True', 'auto_dash_False'),
    )
    def allow_auto_dash_creation(self, request) -> bool:
        return request.param

    @pytest.fixture(
        params=(True, False),
        ids=('manual_counter_True', 'manual_counter_False'),
    )
    def allow_counter_manual_input(self, request) -> bool:
        return request.param


class TestMetricaAPIConnectionForm(MetricaLikeConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = MetricaAPIConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_API_LIB_CONFIGS

    @pytest.fixture
    def connectors_settings(  # noqa
            self,
            allow_auto_dash_creation,
            allow_counter_manual_input,
    ) -> Optional[ConnectorsSettingsByType]:
        return ConnectorsSettingsByType(METRICA=MetricaConnectorSettings(
            COUNTER_ALLOW_MANUAL_INPUT=allow_counter_manual_input,
            ALLOW_AUTO_DASH_CREATION=allow_auto_dash_creation,
        ))


class TestAppMetricaAPIConnectionForm(MetricaLikeConnectionFormTestBase):
    CONN_FORM_FACTORY_CLS = AppMetricaAPIConnectionFormFactory
    TRANSLATION_CONFIGS = BI_API_CONNECTOR_CONFIGS + BI_API_LIB_CONFIGS

    @pytest.fixture
    def connectors_settings(  # noqa
            self,
            allow_auto_dash_creation,
            allow_counter_manual_input,
    ) -> Optional[ConnectorsSettingsByType]:
        return ConnectorsSettingsByType(APPMETRICA=AppmetricaConnectorSettings(
            COUNTER_ALLOW_MANUAL_INPUT=allow_counter_manual_input,
            ALLOW_AUTO_DASH_CREATION=allow_auto_dash_creation,
        ))
