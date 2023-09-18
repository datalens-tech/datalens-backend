from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.core.connector import (
    CHFrozenBaseCoreConnectionDefinition,
    CHFrozenBaseCoreSourceDefinition,
    CHFrozenCoreConnector,
)
from bi_connector_bundle_ch_frozen.ch_frozen_horeca.core.constants import CONNECTION_TYPE_CH_FROZEN_HORECA
from bi_connector_bundle_ch_frozen.ch_frozen_horeca.core.data_source import ClickHouseFrozenHorecaDataSource
from bi_connector_bundle_ch_frozen.ch_frozen_horeca.core.settings import CHFrozenHorecaSettingDefinition


class CHFrozenHorecaCoreConnectionDefinition(CHFrozenBaseCoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_CH_FROZEN_HORECA
    settings_definition = CHFrozenHorecaSettingDefinition


class CHFrozenHorecaCoreSourceDefinition(CHFrozenBaseCoreSourceDefinition):
    source_cls = ClickHouseFrozenHorecaDataSource


class CHFrozenHorecaCoreConnector(CHFrozenCoreConnector):
    connection_definitions = (CHFrozenHorecaCoreConnectionDefinition,)
    source_definitions = (CHFrozenHorecaCoreSourceDefinition,)
