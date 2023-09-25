from dl_api_connector.api_schema.source_base import (
    SQLDataSourceSchema,
    SQLDataSourceTemplateSchema,
    SubselectDataSourceSchema,
    SubselectDataSourceTemplateSchema,
)
from dl_api_connector.connector import (
    ApiConnectionDefinition,
    ApiConnector,
    ApiSourceDefinition,
)

from bi_connector_mysql.bi.api_schema.connection import MySQLConnectionSchema
from bi_connector_mysql.bi.connection_form.form_config import MySQLConnectionFormFactory
from bi_connector_mysql.bi.connection_info import MySQLConnectionInfoProvider
from bi_connector_mysql.bi.i18n.localizer import CONFIGS
from bi_connector_mysql.core.connector import (
    MySQLCoreConnectionDefinition,
    MySQLCoreConnector,
    MySQLSubselectCoreSourceDefinition,
    MySQLTableCoreSourceDefinition,
)
from bi_connector_mysql.formula.constants import DIALECT_NAME_MYSQL


class MySQLApiTableSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = MySQLTableCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class MySQLApiSubselectSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = MySQLSubselectCoreSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class MySQLApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = MySQLCoreConnectionDefinition
    api_generic_schema_cls = MySQLConnectionSchema
    info_provider_cls = MySQLConnectionInfoProvider
    form_factory_cls = MySQLConnectionFormFactory


class MySQLApiConnector(ApiConnector):
    core_connector_cls = MySQLCoreConnector
    connection_definitions = (MySQLApiConnectionDefinition,)
    source_definitions = (
        MySQLApiTableSourceDefinition,
        MySQLApiSubselectSourceDefinition,
    )
    formula_dialect_name = DIALECT_NAME_MYSQL
    translation_configs = frozenset(CONFIGS)
