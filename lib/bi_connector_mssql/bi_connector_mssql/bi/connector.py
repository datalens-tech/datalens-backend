from dl_api_connector.api_schema.source_base import (
    SchematizedSQLDataSourceSchema,
    SchematizedSQLDataSourceTemplateSchema,
    SubselectDataSourceSchema,
    SubselectDataSourceTemplateSchema,
)
from dl_api_connector.connector import (
    BiApiConnectionDefinition,
    BiApiConnector,
    BiApiSourceDefinition,
)

from bi_connector_mssql.bi.api_schema.connection import MSSQLConnectionSchema
from bi_connector_mssql.bi.connection_form.form_config import MSSQLConnectionFormFactory
from bi_connector_mssql.bi.connection_info import MSSQLConnectionInfoProvider
from bi_connector_mssql.bi.i18n.localizer import CONFIGS
from bi_connector_mssql.core.connector import (
    MSSQLCoreConnectionDefinition,
    MSSQLCoreConnector,
    MSSQLSubselectCoreSourceDefinition,
    MSSQLTableCoreSourceDefinition,
)
from bi_connector_mssql.formula.constants import DIALECT_NAME_MSSQLSRV


class MSSQLBiApiTableSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = MSSQLTableCoreSourceDefinition
    api_schema_cls = SchematizedSQLDataSourceSchema
    template_api_schema_cls = SchematizedSQLDataSourceTemplateSchema


class MSSQLBiApiSubselectSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = MSSQLSubselectCoreSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class MSSQLBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = MSSQLCoreConnectionDefinition
    api_generic_schema_cls = MSSQLConnectionSchema
    info_provider_cls = MSSQLConnectionInfoProvider
    form_factory_cls = MSSQLConnectionFormFactory


class MSSQLBiApiConnector(BiApiConnector):
    core_connector_cls = MSSQLCoreConnector
    connection_definitions = (MSSQLBiApiConnectionDefinition,)
    source_definitions = (
        MSSQLBiApiTableSourceDefinition,
        MSSQLBiApiSubselectSourceDefinition,
    )
    formula_dialect_name = DIALECT_NAME_MSSQLSRV
    translation_configs = frozenset(CONFIGS)
