from dl_api_connector.api_schema.source_base import (
    SchematizedSQLDataSourceSchema,
    SchematizedSQLDataSourceTemplateSchema,
    SubselectDataSourceSchema,
    SubselectDataSourceTemplateSchema,
)
from dl_api_connector.connector import (
    ApiConnectionDefinition,
    ApiConnector,
    ApiSourceDefinition,
)

from dl_connector_greenplum.api.api_schema.connection import GreenplumConnectionSchema
from dl_connector_greenplum.api.connection_form.form_config import GreenplumConnectionFormFactory
from dl_connector_greenplum.api.connection_info import GreenplumConnectionInfoProvider
from dl_connector_greenplum.api.i18n.localizer import CONFIGS
from dl_connector_greenplum.core.connector import (
    GreenplumCoreBackendDefinition,
    GreenplumCoreConnectionDefinition,
    GreenplumCoreConnector,
    GreenplumSubselectCoreSourceDefinition,
    GreenplumTableCoreSourceDefinition,
)
from dl_connector_postgresql.api.connector import PostgreSQLApiBackendDefinition
from dl_connector_postgresql.api.i18n.localizer import CONFIGS as BI_CONNECTOR_POSTGRESQL_CONFIGS


class GreenplumApiTableSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = GreenplumTableCoreSourceDefinition
    api_schema_cls = SchematizedSQLDataSourceSchema
    template_api_schema_cls = SchematizedSQLDataSourceTemplateSchema


class GreenplumApiSubselectSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = GreenplumSubselectCoreSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class GreenplumApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = GreenplumCoreConnectionDefinition
    api_generic_schema_cls = GreenplumConnectionSchema
    info_provider_cls = GreenplumConnectionInfoProvider
    form_factory_cls = GreenplumConnectionFormFactory


class GreenplumApiBackendDefinition(PostgreSQLApiBackendDefinition):
    core_backend_definition = GreenplumCoreBackendDefinition


class GreenplumApiConnector(ApiConnector):
    backend_definition = GreenplumApiBackendDefinition
    connection_definitions = (GreenplumApiConnectionDefinition,)
    source_definitions = (
        GreenplumApiTableSourceDefinition,
        GreenplumApiSubselectSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS) | frozenset(BI_CONNECTOR_POSTGRESQL_CONFIGS)
