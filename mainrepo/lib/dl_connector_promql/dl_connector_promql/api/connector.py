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
from dl_connector_promql.api.api_schema.connection import PromQLConnectionSchema
from dl_connector_promql.api.connection_form.form_config import PromQLConnectionFormFactory
from dl_connector_promql.api.connection_info import PromQLConnectionInfoProvider
from dl_connector_promql.api.i18n.localizer import CONFIGS
from dl_connector_promql.core.connector import (
    PromQLCoreConnectionDefinition,
    PromQLCoreConnector,
    PromQLCoreSourceDefinition,
)


class PromQLApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = PromQLCoreConnectionDefinition
    api_generic_schema_cls = PromQLConnectionSchema
    form_factory_cls = PromQLConnectionFormFactory
    info_provider_cls = PromQLConnectionInfoProvider


class PromQLApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = PromQLCoreSourceDefinition
    api_schema_cls = SimpleDataSourceSchema
    template_api_schema_cls = SimpleDataSourceTemplateSchema


class PromQLApiConnector(ApiConnector):
    core_connector_cls = PromQLCoreConnector
    connection_definitions = (PromQLApiConnectionDefinition,)
    source_definitions = (PromQLApiSourceDefinition,)
    translation_configs = frozenset(CONFIGS)
