from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.core.connector import (
    CHFrozenBaseCoreConnectionDefinition,
    CHFrozenBaseCoreSourceDefinition,
    CHFrozenBaseCoreSubselectSourceDefinition,
    CHFrozenCoreConnector,
)
from bi_connector_bundle_ch_frozen.ch_frozen_demo.core.constants import CONNECTION_TYPE_CH_FROZEN_DEMO
from bi_connector_bundle_ch_frozen.ch_frozen_demo.core.data_source import (
    ClickHouseFrozenDemoDataSource,
    ClickHouseFrozenDemoSubselectDataSource,
)
from bi_connector_bundle_ch_frozen.ch_frozen_demo.core.settings import CHFrozenDemoSettingDefinition


class CHFrozenDemoCoreConnectionDefinition(CHFrozenBaseCoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_CH_FROZEN_DEMO
    settings_definition = CHFrozenDemoSettingDefinition


class CHFrozenDemoCoreSourceDefinition(CHFrozenBaseCoreSourceDefinition):
    source_cls = ClickHouseFrozenDemoDataSource


class CHFrozenDemoCoreSubselectSourceDefinition(CHFrozenBaseCoreSubselectSourceDefinition):
    source_cls = ClickHouseFrozenDemoSubselectDataSource


class CHFrozenDemoCoreConnector(CHFrozenCoreConnector):
    connection_definitions = (CHFrozenDemoCoreConnectionDefinition,)
    source_definitions = (  # type: ignore
        CHFrozenDemoCoreSourceDefinition,
        CHFrozenDemoCoreSubselectSourceDefinition,
    )
