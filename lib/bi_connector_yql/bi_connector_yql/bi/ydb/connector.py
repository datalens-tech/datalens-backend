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

from bi_connector_yql.bi.ydb.api_schema.connection import YDBConnectionSchema
from bi_connector_yql.bi.ydb.connection_form.form_config import YDBConnectionFormFactory
from bi_connector_yql.bi.ydb.connection_info import YDBConnectionInfoProvider
from bi_connector_yql.bi.yql_base.i18n.localizer import CONFIGS
from bi_connector_yql.core.ydb.connector import (
    YDBCoreConnectionDefinition,
    YDBCoreConnector,
    YDBCoreSourceDefinition,
    YDBCoreSubselectSourceDefinition,
)
from bi_connector_yql.formula.constants import DIALECT_NAME_YDB


class YDBApiTableSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = YDBCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class YDBApiSubselectSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = YDBCoreSubselectSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class YDBApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = YDBCoreConnectionDefinition
    api_generic_schema_cls = YDBConnectionSchema
    info_provider_cls = YDBConnectionInfoProvider
    form_factory_cls = YDBConnectionFormFactory


class YDBApiConnector(ApiConnector):
    core_connector_cls = YDBCoreConnector
    connection_definitions = (YDBApiConnectionDefinition,)
    source_definitions = (
        YDBApiTableSourceDefinition,
        YDBApiSubselectSourceDefinition,
    )
    formula_dialect_name = DIALECT_NAME_YDB
    translation_configs = frozenset(CONFIGS)
