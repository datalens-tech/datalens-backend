from dl_api_connector.api_schema.source_base import (
    SQLDataSourceSchema,
    SQLDataSourceTemplateSchema,
    SubselectDataSourceSchema,
    SubselectDataSourceTemplateSchema,
)
from dl_api_connector.connector import (
    ApiBackendDefinition,
    ApiConnectionDefinition,
    ApiConnector,
    ApiSourceDefinition,
)
from dl_api_lib.query.registry import MQMFactorySettingItem
from dl_constants.enums import QueryProcessingMode
from dl_query_processing.multi_query.factory import NoCompengMultiQueryMutatorFactory

from dl_connector_mysql.api.api_schema.connection import MySQLConnectionSchema
from dl_connector_mysql.api.connection_form.form_config import MySQLConnectionFormFactory
from dl_connector_mysql.api.connection_info import MySQLConnectionInfoProvider
from dl_connector_mysql.api.i18n.localizer import CONFIGS
from dl_connector_mysql.core.connector import (
    MySQLCoreBackendDefinition,
    MySQLCoreConnectionDefinition,
    MySQLCoreConnector,
    MySQLSubselectCoreSourceDefinition,
    MySQLTableCoreSourceDefinition,
)
from dl_connector_mysql.formula.constants import (
    DIALECT_NAME_MYSQL,
    MySQLDialect,
)


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


class MySQLApiBackendDefinition(ApiBackendDefinition):
    core_backend_definition = MySQLCoreBackendDefinition
    formula_dialect_name = DIALECT_NAME_MYSQL
    multi_query_mutation_factories = ApiBackendDefinition.multi_query_mutation_factories + (
        MQMFactorySettingItem(
            query_proc_mode=QueryProcessingMode.native_wf,
            dialects=MySQLDialect.and_above(MySQLDialect.MYSQL_8_0_12).to_list(),
            factory_cls=NoCompengMultiQueryMutatorFactory,
        ),
    )


class MySQLApiConnector(ApiConnector):
    backend_definition = MySQLApiBackendDefinition
    connection_definitions = (MySQLApiConnectionDefinition,)
    source_definitions = (
        MySQLApiTableSourceDefinition,
        MySQLApiSubselectSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS)
