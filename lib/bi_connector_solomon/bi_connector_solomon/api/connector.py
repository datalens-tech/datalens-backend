from __future__ import annotations

from dl_api_connector.api_schema.source_base import (
    SimpleDataSourceSchema,
    SimpleDataSourceTemplateSchema,
)
from dl_api_connector.connector import (
    ApiConnectionDefinition,
    ApiConnector,
    ApiSourceDefinition,
)

from bi_connector_solomon.api.api_schema.connection import SolomonConnectionSchema
from bi_connector_solomon.api.connection_form.form_config import SolomonConnectionFormFactory
from bi_connector_solomon.api.connection_info import SolomonConnectionInfoProvider
from bi_connector_solomon.api.i18n.localizer import CONFIGS
from bi_connector_solomon.core.connector import (
    SolomonCoreConnectionDefinition,
    SolomonCoreConnector,
    SolomonCoreSourceDefinition,
)


class SolomonApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = SolomonCoreConnectionDefinition
    api_generic_schema_cls = SolomonConnectionSchema
    form_factory_cls = SolomonConnectionFormFactory
    info_provider_cls = SolomonConnectionInfoProvider


class SolomonApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = SolomonCoreSourceDefinition
    api_schema_cls = SimpleDataSourceSchema
    template_api_schema_cls = SimpleDataSourceTemplateSchema


class SolomonApiConnector(ApiConnector):
    core_connector_cls = SolomonCoreConnector
    connection_definitions = (SolomonApiConnectionDefinition,)
    source_definitions = (SolomonApiSourceDefinition,)
    translation_configs = frozenset(CONFIGS)
