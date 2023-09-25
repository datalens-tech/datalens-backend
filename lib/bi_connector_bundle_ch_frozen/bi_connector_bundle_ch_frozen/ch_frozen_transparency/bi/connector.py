from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.connector import (
    BaseCHFrozenApiConnectionDefinition,
    BaseCHFrozenApiConnector,
    BaseCHFrozenTableApiSourceDefinition,
)
from bi_connector_bundle_ch_frozen.ch_frozen_transparency.bi.connection_form.form_config import (
    CHFrozenTransparencyFormFactory,
)
from bi_connector_bundle_ch_frozen.ch_frozen_transparency.bi.connection_info import (
    CHFrozenTransparencyConnectionInfoProvider,
)
from bi_connector_bundle_ch_frozen.ch_frozen_transparency.core.connector import (
    CHFrozenTransparencyCoreConnectionDefinition,
    CHFrozenTransparencyCoreConnector,
    CHFrozenTransparencyCoreSourceDefinition,
)


class CHFrozenTransparencyTableApiSourceDefinition(BaseCHFrozenTableApiSourceDefinition):
    core_source_def_cls = CHFrozenTransparencyCoreSourceDefinition


class CHFrozenTransparencyApiConnectionDefinition(BaseCHFrozenApiConnectionDefinition):
    core_conn_def_cls = CHFrozenTransparencyCoreConnectionDefinition
    form_factory_cls = CHFrozenTransparencyFormFactory
    info_provider_cls = CHFrozenTransparencyConnectionInfoProvider


class CHFrozenTransparencyApiConnector(BaseCHFrozenApiConnector):
    core_connector_cls = CHFrozenTransparencyCoreConnector
    connection_definitions = (CHFrozenTransparencyApiConnectionDefinition,)
    source_definitions = (CHFrozenTransparencyTableApiSourceDefinition,)
