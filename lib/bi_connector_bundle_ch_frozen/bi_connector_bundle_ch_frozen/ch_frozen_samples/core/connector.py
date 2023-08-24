from __future__ import annotations

from bi_configs.connectors_settings import CHFrozenSamplesConnectorSettings
from bi_connector_bundle_ch_frozen.ch_frozen_base.core.connector import (
    CHFrozenBaseCoreConnectionDefinition,
    CHFrozenBaseCoreSourceDefinition,
    CHFrozenCoreConnector,
)
from bi_connector_bundle_ch_frozen.ch_frozen_samples.core.constants import CONNECTION_TYPE_CH_FROZEN_SAMPLES
from bi_connector_bundle_ch_frozen.ch_frozen_samples.core.data_source import ClickHouseFrozenSamplesDataSource


class CHFrozenSamplesCoreConnectionDefinition(CHFrozenBaseCoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_CH_FROZEN_SAMPLES
    settings_class = CHFrozenSamplesConnectorSettings


class CHFrozenSamplesCoreSourceDefinition(CHFrozenBaseCoreSourceDefinition):
    source_cls = ClickHouseFrozenSamplesDataSource


class CHFrozenSamplesCoreConnector(CHFrozenCoreConnector):
    connection_definitions = (
        CHFrozenSamplesCoreConnectionDefinition,
    )
    source_definitions = (
        CHFrozenSamplesCoreSourceDefinition,
    )
