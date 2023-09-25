from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.connector import (
    BaseCHFrozenApiConnectionDefinition,
    BaseCHFrozenApiConnector,
    BaseCHFrozenTableApiSourceDefinition,
)
from bi_connector_bundle_ch_frozen.ch_frozen_dtp.bi.connection_form.form_config import CHFrozenDTPFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_dtp.bi.connection_info import CHFrozenDTPConnectionInfoProvider
from bi_connector_bundle_ch_frozen.ch_frozen_dtp.core.connector import (
    CHFrozenDTPCoreConnectionDefinition,
    CHFrozenDTPCoreConnector,
    CHFrozenDTPCoreSourceDefinition,
)


class CHFrozenDTPTableApiSourceDefinition(BaseCHFrozenTableApiSourceDefinition):
    core_source_def_cls = CHFrozenDTPCoreSourceDefinition


class CHFrozenDTPApiConnectionDefinition(BaseCHFrozenApiConnectionDefinition):
    core_conn_def_cls = CHFrozenDTPCoreConnectionDefinition
    form_factory_cls = CHFrozenDTPFormFactory
    info_provider_cls = CHFrozenDTPConnectionInfoProvider


class CHFrozenDTPApiConnector(BaseCHFrozenApiConnector):
    core_connector_cls = CHFrozenDTPCoreConnector
    connection_definitions = (CHFrozenDTPApiConnectionDefinition,)
    source_definitions = (CHFrozenDTPTableApiSourceDefinition,)
