from __future__ import annotations

from bi_connector_monitoring.core.connector import (
    MonitoringCoreConnectionDefinition,
    MonitoringCoreConnector,
    MonitoringCoreSourceDefinition,
)

from bi_api_connector.api_schema.source_base import (
    SimpleDataSourceSchema,
    SimpleDataSourceTemplateSchema,
)
from bi_api_connector.connector import (
    BiApiConnectionDefinition, BiApiConnector, BiApiSourceDefinition,
)

from bi_api_lib.connectors.monitoring.connection_form.form_config import MonitoringConnectionFormFactory
from bi_api_lib.connectors.monitoring.connection_info import MonitoringConnectionInfoProvider
from bi_api_lib.connectors.monitoring.schemas import MonitoringConnectionSchema


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
