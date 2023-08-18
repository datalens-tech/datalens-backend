from __future__ import annotations

from bi_connector_bundle_ch_frozen.ch_frozen_samples.core.connector import (
    CHFrozenSamplesCoreSourceDefinition,
    CHFrozenSamplesCoreConnectionDefinition,
    CHFrozenSamplesCoreConnector,
)

from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.connector import (
    BaseCHFrozenTableBiApiSourceDefinition,
    BaseCHFrozenBiApiConnectionDefinition,
    BaseCHFrozenBiApiConnector,
)
from bi_connector_bundle_ch_frozen.ch_frozen_samples.bi.connection_form.form_config import CHFrozenSamplesFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_samples.bi.connection_info import CHFrozenSamplesConnectionInfoProvider


class CHFrozenSamplesTableBiApiSourceDefinition(BaseCHFrozenTableBiApiSourceDefinition):
    core_source_def_cls = CHFrozenSamplesCoreSourceDefinition


class CHFrozenSamplesBiApiConnectionDefinition(BaseCHFrozenBiApiConnectionDefinition):
    core_conn_def_cls = CHFrozenSamplesCoreConnectionDefinition
    form_factory_cls = CHFrozenSamplesFormFactory
    info_provider_cls = CHFrozenSamplesConnectionInfoProvider


class CHFrozenSamplesBiApiConnector(BaseCHFrozenBiApiConnector):
    core_connector_cls = CHFrozenSamplesCoreConnector
    connection_definitions = (
        CHFrozenSamplesBiApiConnectionDefinition,
    )
    source_definitions = (
        CHFrozenSamplesTableBiApiSourceDefinition,
    )
