from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.api.connector import (
    BaseCHFrozenApiConnectionDefinition,
    BaseCHFrozenApiConnector,
    BaseCHFrozenTableApiSourceDefinition,
)
from bi_connector_bundle_ch_frozen.ch_frozen_bumpy_roads.api.connection_form.form_config import (
    CHFrozenBumpyRoadsFormFactory,
)
from bi_connector_bundle_ch_frozen.ch_frozen_bumpy_roads.api.connection_info import (
    CHFrozenBumpyRoadsConnectionInfoProvider,
)
from bi_connector_bundle_ch_frozen.ch_frozen_bumpy_roads.core.connector import (
    CHFrozenBumpyRoadsCoreConnectionDefinition,
    CHFrozenBumpyRoadsCoreConnector,
    CHFrozenBumpyRoadsCoreSourceDefinition,
)


class CHFrozenBumpyRoadsTableApiSourceDefinition(BaseCHFrozenTableApiSourceDefinition):
    core_source_def_cls = CHFrozenBumpyRoadsCoreSourceDefinition


class CHFrozenBumpyRoadsApiConnectionDefinition(BaseCHFrozenApiConnectionDefinition):
    core_conn_def_cls = CHFrozenBumpyRoadsCoreConnectionDefinition
    form_factory_cls = CHFrozenBumpyRoadsFormFactory
    info_provider_cls = CHFrozenBumpyRoadsConnectionInfoProvider


class CHFrozenBumpyRoadsApiConnector(BaseCHFrozenApiConnector):
    core_connector_cls = CHFrozenBumpyRoadsCoreConnector
    connection_definitions = (CHFrozenBumpyRoadsApiConnectionDefinition,)
    source_definitions = (CHFrozenBumpyRoadsTableApiSourceDefinition,)
