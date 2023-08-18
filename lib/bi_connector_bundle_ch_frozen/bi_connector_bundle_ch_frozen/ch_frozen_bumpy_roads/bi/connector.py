from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_bumpy_roads.core.connector import (
    CHFrozenBumpyRoadsCoreSourceDefinition,
    CHFrozenBumpyRoadsCoreConnectionDefinition,
    CHFrozenBumpyRoadsCoreConnector,
)

from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.connector import (
    BaseCHFrozenTableBiApiSourceDefinition,
    BaseCHFrozenBiApiConnectionDefinition,
    BaseCHFrozenBiApiConnector,
)
from bi_connector_bundle_ch_frozen.ch_frozen_bumpy_roads.bi.connection_form.form_config import (
    CHFrozenBumpyRoadsFormFactory,
)
from bi_connector_bundle_ch_frozen.ch_frozen_bumpy_roads.bi.connection_info import (
    CHFrozenBumpyRoadsConnectionInfoProvider,
)


class CHFrozenBumpyRoadsTableBiApiSourceDefinition(BaseCHFrozenTableBiApiSourceDefinition):
    core_source_def_cls = CHFrozenBumpyRoadsCoreSourceDefinition


class CHFrozenBumpyRoadsBiApiConnectionDefinition(BaseCHFrozenBiApiConnectionDefinition):
    core_conn_def_cls = CHFrozenBumpyRoadsCoreConnectionDefinition
    form_factory_cls = CHFrozenBumpyRoadsFormFactory
    info_provider_cls = CHFrozenBumpyRoadsConnectionInfoProvider


class CHFrozenBumpyRoadsBiApiConnector(BaseCHFrozenBiApiConnector):
    core_connector_cls = CHFrozenBumpyRoadsCoreConnector
    connection_definitions = (
        CHFrozenBumpyRoadsBiApiConnectionDefinition,
    )
    source_definitions = (
        CHFrozenBumpyRoadsTableBiApiSourceDefinition,
    )
