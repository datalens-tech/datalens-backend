from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.core.connector import (
    CHFrozenBaseCoreConnectionDefinition,
    CHFrozenBaseCoreSourceDefinition,
    CHFrozenCoreConnector,
)
from bi_connector_bundle_ch_frozen.ch_frozen_bumpy_roads.core.constants import CONNECTION_TYPE_CH_FROZEN_BUMPY_ROADS
from bi_connector_bundle_ch_frozen.ch_frozen_bumpy_roads.core.data_source import ClickHouseFrozenBumpyRoadsDataSource


class CHFrozenBumpyRoadsCoreConnectionDefinition(CHFrozenBaseCoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_CH_FROZEN_BUMPY_ROADS


class CHFrozenBumpyRoadsCoreSourceDefinition(CHFrozenBaseCoreSourceDefinition):
    source_cls = ClickHouseFrozenBumpyRoadsDataSource


class CHFrozenBumpyRoadsCoreConnector(CHFrozenCoreConnector):
    connection_definitions = (
        CHFrozenBumpyRoadsCoreConnectionDefinition,
    )
    source_definitions = (
        CHFrozenBumpyRoadsCoreSourceDefinition,
    )
