from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.api.connector import (
    BaseCHFrozenApiConnectionDefinition,
    BaseCHFrozenApiConnector,
    BaseCHFrozenTableApiSourceDefinition,
)
from bi_connector_bundle_ch_frozen.ch_frozen_gkh.api.connection_form.form_config import CHFrozenGKHFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_gkh.api.connection_info import CHFrozenGKHConnectionInfoProvider
from bi_connector_bundle_ch_frozen.ch_frozen_gkh.core.connector import (
    CHFrozenGKHCoreConnectionDefinition,
    CHFrozenGKHCoreConnector,
    CHFrozenGKHCoreSourceDefinition,
)


class CHFrozenGKHTableApiSourceDefinition(BaseCHFrozenTableApiSourceDefinition):
    core_source_def_cls = CHFrozenGKHCoreSourceDefinition


class CHFrozenGKHApiConnectionDefinition(BaseCHFrozenApiConnectionDefinition):
    core_conn_def_cls = CHFrozenGKHCoreConnectionDefinition
    form_factory_cls = CHFrozenGKHFormFactory
    info_provider_cls = CHFrozenGKHConnectionInfoProvider


class CHFrozenGKHApiConnector(BaseCHFrozenApiConnector):
    core_connector_cls = CHFrozenGKHCoreConnector
    connection_definitions = (CHFrozenGKHApiConnectionDefinition,)
    source_definitions = (CHFrozenGKHTableApiSourceDefinition,)
