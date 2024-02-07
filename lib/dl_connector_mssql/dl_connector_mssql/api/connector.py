from dl_api_connector.api_schema.source_base import (
    SchematizedSQLDataSourceSchema,
    SchematizedSQLDataSourceTemplateSchema,
    SubselectDataSourceSchema,
    SubselectDataSourceTemplateSchema,
)
from dl_api_connector.connector import (
    ApiBackendDefinition,
    ApiConnectionDefinition,
    ApiConnector,
    ApiSourceDefinition,
)

from dl_connector_mssql.api.api_schema.connection import MSSQLConnectionSchema
from dl_connector_mssql.api.connection_form.form_config import MSSQLConnectionFormFactory
from dl_connector_mssql.api.connection_info import MSSQLConnectionInfoProvider
from dl_connector_mssql.api.i18n.localizer import CONFIGS
from dl_connector_mssql.core.connector import (
    MSSQLCoreBackendDefinition,
    MSSQLCoreConnectionDefinition,
    MSSQLSubselectCoreSourceDefinition,
    MSSQLTableCoreSourceDefinition,
)
from dl_connector_mssql.formula.constants import DIALECT_NAME_MSSQLSRV


class MSSQLApiTableSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = MSSQLTableCoreSourceDefinition
    api_schema_cls = SchematizedSQLDataSourceSchema
    template_api_schema_cls = SchematizedSQLDataSourceTemplateSchema


class MSSQLApiSubselectSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = MSSQLSubselectCoreSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class MSSQLApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = MSSQLCoreConnectionDefinition
    api_generic_schema_cls = MSSQLConnectionSchema
    info_provider_cls = MSSQLConnectionInfoProvider
    form_factory_cls = MSSQLConnectionFormFactory


class MSSQLApiBackendDefinition(ApiBackendDefinition):
    core_backend_definition = MSSQLCoreBackendDefinition
    formula_dialect_name = DIALECT_NAME_MSSQLSRV


class MSSQLApiConnector(ApiConnector):
    backend_definition = MSSQLApiBackendDefinition
    connection_definitions = (MSSQLApiConnectionDefinition,)
    source_definitions = (
        MSSQLApiTableSourceDefinition,
        MSSQLApiSubselectSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS)
