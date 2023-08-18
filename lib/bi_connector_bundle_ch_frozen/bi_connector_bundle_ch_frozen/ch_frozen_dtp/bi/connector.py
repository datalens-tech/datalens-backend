from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_dtp.core.connector import (
    CHFrozenDTPCoreSourceDefinition,
    CHFrozenDTPCoreConnectionDefinition,
    CHFrozenDTPCoreConnector,
)

from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.connector import (
    BaseCHFrozenTableBiApiSourceDefinition,
    BaseCHFrozenBiApiConnectionDefinition,
    BaseCHFrozenBiApiConnector,
)
from bi_connector_bundle_ch_frozen.ch_frozen_dtp.bi.connection_form.form_config import CHFrozenDTPFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_dtp.bi.connection_info import CHFrozenDTPConnectionInfoProvider


class CHFrozenDTPTableBiApiSourceDefinition(BaseCHFrozenTableBiApiSourceDefinition):
    core_source_def_cls = CHFrozenDTPCoreSourceDefinition


class CHFrozenDTPBiApiConnectionDefinition(BaseCHFrozenBiApiConnectionDefinition):
    core_conn_def_cls = CHFrozenDTPCoreConnectionDefinition
    form_factory_cls = CHFrozenDTPFormFactory
    info_provider_cls = CHFrozenDTPConnectionInfoProvider


class CHFrozenDTPBiApiConnector(BaseCHFrozenBiApiConnector):
    core_connector_cls = CHFrozenDTPCoreConnector
    connection_definitions = (
        CHFrozenDTPBiApiConnectionDefinition,
    )
    source_definitions = (
        CHFrozenDTPTableBiApiSourceDefinition,
    )
