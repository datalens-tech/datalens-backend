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

from dl_connector_ydb.api.ydb.api_schema.connection import YDBConnectionSchema
from dl_connector_ydb.api.ydb.connection_form.form_config import YDBConnectionFormFactory
from dl_connector_ydb.api.ydb.connection_info import YDBConnectionInfoProvider
from dl_connector_ydb.api.ydb.i18n.localizer import CONFIGS
from dl_connector_ydb.core.ydb.connector import (
    YDBCoreBackendDefinition,
    YDBCoreConnectionDefinition,
    YDBCoreSourceDefinition,
    YDBCoreSubselectSourceDefinition,
)
from dl_connector_ydb.formula.constants import DIALECT_NAME_YDB


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


class YDBApiBackendDefinition(ApiBackendDefinition):
    core_backend_definition = YDBCoreBackendDefinition
    formula_dialect_name = DIALECT_NAME_YDB


class YDBApiConnector(ApiConnector):
    backend_definition = YDBApiBackendDefinition
    connection_definitions = (YDBApiConnectionDefinition,)
    source_definitions = (
        YDBApiTableSourceDefinition,
        YDBApiSubselectSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS)
