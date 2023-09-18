from __future__ import annotations

from bi_connector_monitoring.core.connector import (
    MonitoringCoreConnectionDefinition,
    MonitoringCoreConnector,
    MonitoringCoreSourceDefinition,
)

from dl_api_connector.api_schema.source_base import (
    SimpleDataSourceSchema,
    SimpleDataSourceTemplateSchema,
)
from dl_api_connector.connector import (
    BiApiConnectionDefinition, BiApiConnector, BiApiSourceDefinition,
)

from bi_connector_monitoring.bi.api_schema.connection import MonitoringConnectionSchema
from bi_connector_monitoring.bi.connection_form.form_config import MonitoringConnectionFormFactory
from bi_connector_monitoring.bi.connection_info import MonitoringConnectionInfoProvider
from bi_connector_monitoring.bi.i18n.localizer import CONFIGS


class MonitoringBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = MonitoringCoreConnectionDefinition
    api_generic_schema_cls = MonitoringConnectionSchema
    form_factory_cls = MonitoringConnectionFormFactory
    info_provider_cls = MonitoringConnectionInfoProvider


class MonitoringBiApiSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = MonitoringCoreSourceDefinition
    api_schema_cls = SimpleDataSourceSchema
    template_api_schema_cls = SimpleDataSourceTemplateSchema


class MonitoringBiApiConnector(BiApiConnector):
    core_connector_cls = MonitoringCoreConnector
    connection_definitions = (
        MonitoringBiApiConnectionDefinition,
    )
    source_definitions = (
        MonitoringBiApiSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS)
