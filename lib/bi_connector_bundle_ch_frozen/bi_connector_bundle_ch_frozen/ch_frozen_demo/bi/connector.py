from __future__ import annotations

from dl_api_connector.api_schema.source_base import (
    SubselectDataSourceSchema,
    SubselectDataSourceTemplateSchema,
)

from bi_connector_bundle_ch_frozen.ch_frozen_base.bi.connector import (
    BaseCHFrozenBiApiConnectionDefinition,
    BaseCHFrozenBiApiConnector,
    BaseCHFrozenTableBiApiSourceDefinition,
)
from bi_connector_bundle_ch_frozen.ch_frozen_demo.bi.connection_form.form_config import CHFrozenDemoFormFactory
from bi_connector_bundle_ch_frozen.ch_frozen_demo.bi.connection_info import CHFrozenDemoConnectionInfoProvider
from bi_connector_bundle_ch_frozen.ch_frozen_demo.core.connector import (
    CHFrozenDemoCoreConnectionDefinition,
    CHFrozenDemoCoreConnector,
    CHFrozenDemoCoreSourceDefinition,
    CHFrozenDemoCoreSubselectSourceDefinition,
)


class CHFrozenDemoTableBiApiSourceDefinition(BaseCHFrozenTableBiApiSourceDefinition):
    core_source_def_cls = CHFrozenDemoCoreSourceDefinition


class CHFrozenDemoBiApiSubselectSourceDefinition(BaseCHFrozenTableBiApiSourceDefinition):
    core_source_def_cls = CHFrozenDemoCoreSubselectSourceDefinition  # type: ignore
    api_schema_cls = SubselectDataSourceSchema  # type: ignore
    template_api_schema_cls = SubselectDataSourceTemplateSchema  # type: ignore


class CHFrozenDemoBiApiConnectionDefinition(BaseCHFrozenBiApiConnectionDefinition):
    core_conn_def_cls = CHFrozenDemoCoreConnectionDefinition
    form_factory_cls = CHFrozenDemoFormFactory
    info_provider_cls = CHFrozenDemoConnectionInfoProvider


class CHFrozenDemoBiApiConnector(BaseCHFrozenBiApiConnector):
    core_connector_cls = CHFrozenDemoCoreConnector
    connection_definitions = (CHFrozenDemoBiApiConnectionDefinition,)
    source_definitions = (  # type: ignore
        CHFrozenDemoTableBiApiSourceDefinition,
        CHFrozenDemoBiApiSubselectSourceDefinition,
    )
