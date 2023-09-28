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

from bi_connector_monitoring.api.api_schema.connection import MonitoringConnectionSchema
from bi_connector_monitoring.api.connection_form.form_config import MonitoringConnectionFormFactory
from bi_connector_monitoring.api.connection_info import MonitoringConnectionInfoProvider
from bi_connector_monitoring.api.i18n.localizer import CONFIGS
from bi_connector_monitoring.core.connector import (
    MonitoringCoreConnectionDefinition,
    MonitoringCoreConnector,
    MonitoringCoreSourceDefinition,
)


class MonitoringApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = MonitoringCoreConnectionDefinition
    api_generic_schema_cls = MonitoringConnectionSchema
    form_factory_cls = MonitoringConnectionFormFactory
    info_provider_cls = MonitoringConnectionInfoProvider


class MonitoringApiSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = MonitoringCoreSourceDefinition
    api_schema_cls = SimpleDataSourceSchema
    template_api_schema_cls = SimpleDataSourceTemplateSchema


class MonitoringApiConnector(ApiConnector):
    core_connector_cls = MonitoringCoreConnector
    connection_definitions = (MonitoringApiConnectionDefinition,)
    source_definitions = (MonitoringApiSourceDefinition,)
    translation_configs = frozenset(CONFIGS)
