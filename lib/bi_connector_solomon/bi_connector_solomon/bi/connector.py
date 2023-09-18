from __future__ import annotations

from dl_api_connector.connector import (
    BiApiConnectionDefinition, BiApiConnector, BiApiSourceDefinition,
)
from dl_api_connector.api_schema.source_base import (
    SimpleDataSourceSchema,
    SimpleDataSourceTemplateSchema,
)

from bi_connector_solomon.bi.api_schema.connection import SolomonConnectionSchema
from bi_connector_solomon.bi.connection_form.form_config import SolomonConnectionFormFactory
from bi_connector_solomon.bi.connection_info import SolomonConnectionInfoProvider
from bi_connector_solomon.bi.i18n.localizer import CONFIGS
from bi_connector_solomon.core.connector import (
    SolomonCoreConnectionDefinition,
    SolomonCoreConnector,
    SolomonCoreSourceDefinition,
)


class SolomonBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = SolomonCoreConnectionDefinition
    api_generic_schema_cls = SolomonConnectionSchema
    form_factory_cls = SolomonConnectionFormFactory
    info_provider_cls = SolomonConnectionInfoProvider


class SolomonBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = SolomonCoreSourceDefinition
    api_schema_cls = SimpleDataSourceSchema
    template_api_schema_cls = SimpleDataSourceTemplateSchema


class SolomonBiApiConnector(BiApiConnector):
    core_connector_cls = SolomonCoreConnector
    connection_definitions = (
        SolomonBiApiConnectionDefinition,
    )
    source_definitions = (
        SolomonBiApiSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS)
