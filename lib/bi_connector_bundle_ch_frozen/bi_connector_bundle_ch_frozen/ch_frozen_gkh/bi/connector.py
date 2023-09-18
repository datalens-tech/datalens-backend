from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.connector import (
    BaseCHFrozenBiApiConnectionDefinition,
    BaseCHFrozenBiApiConnector,
    BaseCHFrozenTableBiApiSourceDefinition,
)
from bi_connector_bundle_ch_frozen.ch_frozen_gkh.bi.connection_form.form_config import CHFrozenGKHFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_gkh.bi.connection_info import CHFrozenGKHConnectionInfoProvider
from bi_connector_bundle_ch_frozen.ch_frozen_gkh.core.connector import (
    CHFrozenGKHCoreConnectionDefinition,
    CHFrozenGKHCoreConnector,
    CHFrozenGKHCoreSourceDefinition,
)


class CHFrozenGKHTableBiApiSourceDefinition(BaseCHFrozenTableBiApiSourceDefinition):
    core_source_def_cls = CHFrozenGKHCoreSourceDefinition


class CHFrozenGKHBiApiConnectionDefinition(BaseCHFrozenBiApiConnectionDefinition):
    core_conn_def_cls = CHFrozenGKHCoreConnectionDefinition
    form_factory_cls = CHFrozenGKHFormFactory
    info_provider_cls = CHFrozenGKHConnectionInfoProvider


class CHFrozenGKHBiApiConnector(BaseCHFrozenBiApiConnector):
    core_connector_cls = CHFrozenGKHCoreConnector
    connection_definitions = (CHFrozenGKHBiApiConnectionDefinition,)
    source_definitions = (CHFrozenGKHTableBiApiSourceDefinition,)
