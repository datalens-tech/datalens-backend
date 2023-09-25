from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.connector import (
    BaseCHFrozenApiConnectionDefinition,
    BaseCHFrozenApiConnector,
    BaseCHFrozenTableApiSourceDefinition,
)
from bi_connector_bundle_ch_frozen.ch_frozen_horeca.bi.connection_form.form_config import CHFrozenHorecaFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_horeca.bi.connection_info import CHFrozenHorecaConnectionInfoProvider
from bi_connector_bundle_ch_frozen.ch_frozen_horeca.core.connector import (
    CHFrozenHorecaCoreConnectionDefinition,
    CHFrozenHorecaCoreConnector,
    CHFrozenHorecaCoreSourceDefinition,
)


class CHFrozenHorecaTableApiSourceDefinition(BaseCHFrozenTableApiSourceDefinition):
    core_source_def_cls = CHFrozenHorecaCoreSourceDefinition


class CHFrozenHorecaApiConnectionDefinition(BaseCHFrozenApiConnectionDefinition):
    core_conn_def_cls = CHFrozenHorecaCoreConnectionDefinition
    form_factory_cls = CHFrozenHorecaFormFactory
    info_provider_cls = CHFrozenHorecaConnectionInfoProvider


class CHFrozenHorecaApiConnector(BaseCHFrozenApiConnector):
    core_connector_cls = CHFrozenHorecaCoreConnector
    connection_definitions = (CHFrozenHorecaApiConnectionDefinition,)
    source_definitions = (CHFrozenHorecaTableApiSourceDefinition,)
