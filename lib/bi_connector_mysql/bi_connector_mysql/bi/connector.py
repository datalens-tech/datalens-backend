from bi_connector_mysql.core.connector import (
    MySQLCoreConnector,
    MySQLCoreConnectionDefinition,
    MySQLTableCoreSourceDefinition,
    MySQLSubselectCoreSourceDefinition,
)
from bi_formula.core.dialect import DialectName

from bi_api_connector.connector import (
    BiApiSourceDefinition,
    BiApiConnectionDefinition,
    BiApiConnector,
)

from bi_api_connector.api_schema.source_base import (
    SQLDataSourceSchema, SQLDataSourceTemplateSchema, SubselectDataSourceSchema, SubselectDataSourceTemplateSchema,
)
from bi_connector_mysql.bi.api_schema.connection import MySQLConnectionSchema
from bi_connector_mysql.bi.connection_form.form_config import MySQLConnectionFormFactory
from bi_connector_mysql.bi.connection_info import MySQLConnectionInfoProvider
from bi_connector_mysql.bi.i18n.localizer import CONFIGS


class MySQLBiApiTableSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = MySQLTableCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class MySQLBiApiSubselectSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = MySQLSubselectCoreSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class MySQLBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = MySQLCoreConnectionDefinition
    api_generic_schema_cls = MySQLConnectionSchema
    info_provider_cls = MySQLConnectionInfoProvider
    form_factory_cls = MySQLConnectionFormFactory


class MySQLBiApiConnector(BiApiConnector):
    core_connector_cls = MySQLCoreConnector
    connection_definitions = (MySQLBiApiConnectionDefinition,)
    source_definitions = (
        MySQLBiApiTableSourceDefinition,
        MySQLBiApiSubselectSourceDefinition,
    )
    formula_dialect_name = DialectName.MYSQL
    translation_configs = frozenset(CONFIGS)
