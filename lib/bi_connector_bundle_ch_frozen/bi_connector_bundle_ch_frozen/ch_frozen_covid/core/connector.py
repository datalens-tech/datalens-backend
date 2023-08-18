from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.core.connector import (
    CHFrozenBaseCoreConnectionDefinition,
    CHFrozenBaseCoreSourceDefinition,
    CHFrozenCoreConnector,
)
from bi_connector_bundle_ch_frozen.ch_frozen_covid.core.constants import CONNECTION_TYPE_CH_FROZEN_COVID
from bi_connector_bundle_ch_frozen.ch_frozen_covid.core.data_source import ClickHouseFrozenCovidDataSource


class CHFrozenCovidCoreConnectionDefinition(CHFrozenBaseCoreConnectionDefinition):
    conn_type = CONNECTION_TYPE_CH_FROZEN_COVID


class CHFrozenCovidCoreSourceDefinition(CHFrozenBaseCoreSourceDefinition):
    source_cls = ClickHouseFrozenCovidDataSource


class CHFrozenCovidCoreConnector(CHFrozenCoreConnector):
    connection_definitions = (
        CHFrozenCovidCoreConnectionDefinition,
    )
    source_definitions = (
        CHFrozenCovidCoreSourceDefinition,
    )
