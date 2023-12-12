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
from dl_api_lib.query.registry import MQMFactorySettingItem
from dl_constants.enums import QueryProcessingMode
from dl_query_processing.multi_query.factory import NoCompengMultiQueryMutatorFactory

from dl_connector_postgresql.api.api_schema.connection import PostgreSQLConnectionSchema
from dl_connector_postgresql.api.connection_form.form_config import PostgreSQLConnectionFormFactory
from dl_connector_postgresql.api.connection_info import PostgreSQLConnectionInfoProvider
from dl_connector_postgresql.api.i18n.localizer import CONFIGS
from dl_connector_postgresql.core.postgresql.connector import (
    PostgreSQLCoreBackendDefinition,
    PostgreSQLCoreConnectionDefinition,
    PostgreSQLCoreConnector,
    PostgreSQLSubselectCoreSourceDefinition,
    PostgreSQLTableCoreSourceDefinition,
)
from dl_connector_postgresql.formula.constants import (
    DIALECT_NAME_POSTGRESQL,
    PostgreSQLDialect,
)


class PostgreSQLApiTableSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = PostgreSQLTableCoreSourceDefinition
    api_schema_cls = SchematizedSQLDataSourceSchema
    template_api_schema_cls = SchematizedSQLDataSourceTemplateSchema


class PostgreSQLApiSubselectSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = PostgreSQLSubselectCoreSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class PostgreSQLApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = PostgreSQLCoreConnectionDefinition
    api_generic_schema_cls = PostgreSQLConnectionSchema
    info_provider_cls = PostgreSQLConnectionInfoProvider
    form_factory_cls = PostgreSQLConnectionFormFactory


class PostgreSQLApiBackendDefinition(ApiBackendDefinition):
    core_backend_definition = PostgreSQLCoreBackendDefinition
    formula_dialect_name = DIALECT_NAME_POSTGRESQL
    multi_query_mutation_factories = ApiBackendDefinition.multi_query_mutation_factories + (
        MQMFactorySettingItem(
            query_proc_mode=QueryProcessingMode.native_wf,
            dialects=PostgreSQLDialect.and_above(PostgreSQLDialect.POSTGRESQL_9_4).to_list(),
            factory_cls=NoCompengMultiQueryMutatorFactory,
        ),
    )


class PostgreSQLApiConnector(ApiConnector):
    backend_definition = PostgreSQLApiBackendDefinition
    connection_definitions = (PostgreSQLApiConnectionDefinition,)
    source_definitions = (
        PostgreSQLApiTableSourceDefinition,
        PostgreSQLApiSubselectSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS)
    compeng_dialect = PostgreSQLDialect.COMPENG
