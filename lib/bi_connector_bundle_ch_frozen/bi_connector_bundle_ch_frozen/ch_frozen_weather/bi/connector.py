from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.connector import (
    BaseCHFrozenBiApiConnectionDefinition,
    BaseCHFrozenBiApiConnector,
    BaseCHFrozenTableBiApiSourceDefinition,
)
from bi_connector_bundle_ch_frozen.ch_frozen_weather.bi.connection_form.form_config import CHFrozenWeatherFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_weather.bi.connection_info import CHFrozenWeatherConnectionInfoProvider
from bi_connector_bundle_ch_frozen.ch_frozen_weather.core.connector import (
    CHFrozenWeatherCoreConnectionDefinition,
    CHFrozenWeatherCoreConnector,
    CHFrozenWeatherCoreSourceDefinition,
)


class CHFrozenWeatherTableBiApiSourceDefinition(BaseCHFrozenTableBiApiSourceDefinition):
    core_source_def_cls = CHFrozenWeatherCoreSourceDefinition


class CHFrozenWeatherBiApiConnectionDefinition(BaseCHFrozenBiApiConnectionDefinition):
    core_conn_def_cls = CHFrozenWeatherCoreConnectionDefinition
    form_factory_cls = CHFrozenWeatherFormFactory
    info_provider_cls = CHFrozenWeatherConnectionInfoProvider


class CHFrozenWeatherBiApiConnector(BaseCHFrozenBiApiConnector):
    core_connector_cls = CHFrozenWeatherCoreConnector
    connection_definitions = (CHFrozenWeatherBiApiConnectionDefinition,)
    source_definitions = (CHFrozenWeatherTableBiApiSourceDefinition,)
