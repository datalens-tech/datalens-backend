from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_transparency.core.connector import (
    CHFrozenTransparencyCoreSourceDefinition,
    CHFrozenTransparencyCoreConnectionDefinition,
    CHFrozenTransparencyCoreConnector,
)

from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.connector import (
    BaseCHFrozenTableBiApiSourceDefinition,
    BaseCHFrozenBiApiConnectionDefinition,
    BaseCHFrozenBiApiConnector,
)
from bi_connector_bundle_ch_frozen.ch_frozen_transparency.bi.connection_form.form_config import (
    CHFrozenTransparencyFormFactory,
)
from bi_connector_bundle_ch_frozen.ch_frozen_transparency.bi.connection_info import (
    CHFrozenTransparencyConnectionInfoProvider,
)


class CHFrozenTransparencyTableBiApiSourceDefinition(BaseCHFrozenTableBiApiSourceDefinition):
    core_source_def_cls = CHFrozenTransparencyCoreSourceDefinition


class CHFrozenTransparencyBiApiConnectionDefinition(BaseCHFrozenBiApiConnectionDefinition):
    core_conn_def_cls = CHFrozenTransparencyCoreConnectionDefinition
    form_factory_cls = CHFrozenTransparencyFormFactory
    info_provider_cls = CHFrozenTransparencyConnectionInfoProvider


class CHFrozenTransparencyBiApiConnector(BaseCHFrozenBiApiConnector):
    core_connector_cls = CHFrozenTransparencyCoreConnector
    connection_definitions = (
        CHFrozenTransparencyBiApiConnectionDefinition,
    )
    source_definitions = (
        CHFrozenTransparencyTableBiApiSourceDefinition,
    )
