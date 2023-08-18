from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_covid.core.connector import (
    CHFrozenCovidCoreSourceDefinition,
    CHFrozenCovidCoreConnectionDefinition,
    CHFrozenCovidCoreConnector,
)

from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.connector import (
    BaseCHFrozenTableBiApiSourceDefinition,
    BaseCHFrozenBiApiConnectionDefinition,
    BaseCHFrozenBiApiConnector,
)
from bi_connector_bundle_ch_frozen.ch_frozen_covid.bi.connection_form.form_config import CHFrozenCovidFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_covid.bi.connection_info import CHFrozenCovidConnectionInfoProvider


class CHFrozenCovidTableBiApiSourceDefinition(BaseCHFrozenTableBiApiSourceDefinition):
    core_source_def_cls = CHFrozenCovidCoreSourceDefinition


class CHFrozenCovidBiApiConnectionDefinition(BaseCHFrozenBiApiConnectionDefinition):
    core_conn_def_cls = CHFrozenCovidCoreConnectionDefinition
    form_factory_cls = CHFrozenCovidFormFactory
    info_provider_cls = CHFrozenCovidConnectionInfoProvider


class CHFrozenCovidBiApiConnector(BaseCHFrozenBiApiConnector):
    core_connector_cls = CHFrozenCovidCoreConnector
    connection_definitions = (
        CHFrozenCovidBiApiConnectionDefinition,
    )
    source_definitions = (
        CHFrozenCovidTableBiApiSourceDefinition,
    )
