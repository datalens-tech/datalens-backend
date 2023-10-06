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
    GreenplumCoreConnectionDefinition,
    GreenplumCoreConnector,
    GreenplumSubselectCoreSourceDefinition,
    GreenplumTableCoreSourceDefinition,
)
from dl_connector_postgresql.formula.constants import DIALECT_NAME_POSTGRESQL


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


class GreenplumApiConnector(ApiConnector):
    core_connector_cls = GreenplumCoreConnector
    connection_definitions = (GreenplumApiConnectionDefinition,)
    source_definitions = (
        GreenplumApiTableSourceDefinition,
        GreenplumApiSubselectSourceDefinition,
    )
    formula_dialect_name = DIALECT_NAME_POSTGRESQL
    translation_configs = frozenset(CONFIGS)
