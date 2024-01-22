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

from dl_connector_oracle.api.api_schema.connection import OracleConnectionSchema
from dl_connector_oracle.api.connection_form.form_config import OracleConnectionFormFactory
from dl_connector_oracle.api.connection_info import OracleConnectionInfoProvider
from dl_connector_oracle.api.i18n.localizer import CONFIGS
from dl_connector_oracle.core.connector import (
    OracleCoreBackendDefinition,
    OracleCoreConnectionDefinition,
    OracleSubselectCoreSourceDefinition,
    OracleTableCoreSourceDefinition,
)
from dl_connector_oracle.formula.constants import DIALECT_NAME_ORACLE


class OracleApiTableSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = OracleTableCoreSourceDefinition
    api_schema_cls = SchematizedSQLDataSourceSchema
    template_api_schema_cls = SchematizedSQLDataSourceTemplateSchema


class OracleApiSubselectSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = OracleSubselectCoreSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class OracleApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = OracleCoreConnectionDefinition
    api_generic_schema_cls = OracleConnectionSchema
    info_provider_cls = OracleConnectionInfoProvider
    form_factory_cls = OracleConnectionFormFactory


class OracleApiBackendDefinition(ApiBackendDefinition):
    core_backend_definition = OracleCoreBackendDefinition
    formula_dialect_name = DIALECT_NAME_ORACLE


class OracleApiConnector(ApiConnector):
    connection_definitions = (OracleApiConnectionDefinition,)
    backend_definition = OracleApiBackendDefinition
    source_definitions = (
        OracleApiTableSourceDefinition,
        OracleApiSubselectSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS)
