from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.connector import (
    BaseCHFrozenBiApiConnectionDefinition,
    BaseCHFrozenBiApiConnector,
    BaseCHFrozenTableBiApiSourceDefinition,
)
from bi_connector_bundle_ch_frozen.ch_frozen_horeca.bi.connection_form.form_config import CHFrozenHorecaFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_horeca.bi.connection_info import CHFrozenHorecaConnectionInfoProvider
from bi_connector_bundle_ch_frozen.ch_frozen_horeca.core.connector import (
    CHFrozenHorecaCoreConnectionDefinition,
    CHFrozenHorecaCoreConnector,
    CHFrozenHorecaCoreSourceDefinition,
)


class CHFrozenHorecaTableBiApiSourceDefinition(BaseCHFrozenTableBiApiSourceDefinition):
    core_source_def_cls = CHFrozenHorecaCoreSourceDefinition


class CHFrozenHorecaBiApiConnectionDefinition(BaseCHFrozenBiApiConnectionDefinition):
    core_conn_def_cls = CHFrozenHorecaCoreConnectionDefinition
    form_factory_cls = CHFrozenHorecaFormFactory
    info_provider_cls = CHFrozenHorecaConnectionInfoProvider


class CHFrozenHorecaBiApiConnector(BaseCHFrozenBiApiConnector):
    core_connector_cls = CHFrozenHorecaCoreConnector
    connection_definitions = (CHFrozenHorecaBiApiConnectionDefinition,)
    source_definitions = (CHFrozenHorecaTableBiApiSourceDefinition,)
