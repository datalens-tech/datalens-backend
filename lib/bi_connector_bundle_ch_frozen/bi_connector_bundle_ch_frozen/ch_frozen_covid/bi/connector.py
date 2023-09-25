from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.connector import (
    BaseCHFrozenApiConnectionDefinition,
    BaseCHFrozenApiConnector,
    BaseCHFrozenTableApiSourceDefinition,
)
from bi_connector_bundle_ch_frozen.ch_frozen_covid.bi.connection_form.form_config import CHFrozenCovidFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_covid.bi.connection_info import CHFrozenCovidConnectionInfoProvider
from bi_connector_bundle_ch_frozen.ch_frozen_covid.core.connector import (
    CHFrozenCovidCoreConnectionDefinition,
    CHFrozenCovidCoreConnector,
    CHFrozenCovidCoreSourceDefinition,
)


class CHFrozenCovidTableApiSourceDefinition(BaseCHFrozenTableApiSourceDefinition):
    core_source_def_cls = CHFrozenCovidCoreSourceDefinition


class CHFrozenCovidApiConnectionDefinition(BaseCHFrozenApiConnectionDefinition):
    core_conn_def_cls = CHFrozenCovidCoreConnectionDefinition
    form_factory_cls = CHFrozenCovidFormFactory
    info_provider_cls = CHFrozenCovidConnectionInfoProvider


class CHFrozenCovidApiConnector(BaseCHFrozenApiConnector):
    core_connector_cls = CHFrozenCovidCoreConnector
    connection_definitions = (CHFrozenCovidApiConnectionDefinition,)
    source_definitions = (CHFrozenCovidTableApiSourceDefinition,)
