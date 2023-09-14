from __future__ import annotations

from bi_api_connector.api_schema.source_base import (
    SimpleDataSourceSchema,
    SimpleDataSourceTemplateSchema,
)
from bi_api_connector.connector import (
    BiApiConnectionDefinition,
    BiApiConnector,
    BiApiSourceDefinition,
)

from bi_connector_promql.bi.api_schema.connection import PromQLConnectionSchema
from bi_connector_promql.bi.connection_form.form_config import PromQLConnectionFormFactory
from bi_connector_promql.bi.connection_info import PromQLConnectionInfoProvider
from bi_connector_promql.bi.i18n.localizer import CONFIGS
from bi_connector_promql.core.connector import (
    PromQLCoreConnectionDefinition,
    PromQLCoreConnector,
    PromQLCoreSourceDefinition,
)


class PromQLBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = PromQLCoreConnectionDefinition
    api_generic_schema_cls = PromQLConnectionSchema
    form_factory_cls = PromQLConnectionFormFactory
    info_provider_cls = PromQLConnectionInfoProvider


class PromQLBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = PromQLCoreSourceDefinition
    api_schema_cls = SimpleDataSourceSchema
    template_api_schema_cls = SimpleDataSourceTemplateSchema


class PromQLBiApiConnector(BiApiConnector):
    core_connector_cls = PromQLCoreConnector
    connection_definitions = (PromQLBiApiConnectionDefinition,)
    source_definitions = (PromQLBiApiSourceDefinition,)
    translation_configs = frozenset(CONFIGS)
