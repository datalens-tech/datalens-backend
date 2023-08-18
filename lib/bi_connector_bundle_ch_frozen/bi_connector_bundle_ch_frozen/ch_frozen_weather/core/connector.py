from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.core.connector import (
    CHFrozenBaseCoreConnectionDefinition,
    CHFrozenBaseCoreSourceDefinition,
    CHFrozenCoreConnector,
)
from bi_connector_bundle_ch_frozen.ch_frozen_weather.core.constants import CONNECTION_TYPE_CH_FROZEN_WEATHER
from bi_connector_bundle_ch_frozen.ch_frozen_weather.core.data_source import ClickHouseFrozenWeatherDataSource


class CHFrozenWeatherCoreConnectionDefinition(CHFrozenBaseCoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_CH_FROZEN_WEATHER


class CHFrozenWeatherCoreSourceDefinition(CHFrozenBaseCoreSourceDefinition):
    source_cls = ClickHouseFrozenWeatherDataSource


class CHFrozenWeatherCoreConnector(CHFrozenCoreConnector):
    connection_definitions = (
        CHFrozenWeatherCoreConnectionDefinition,
    )
    source_definitions = (
        CHFrozenWeatherCoreSourceDefinition,
    )
