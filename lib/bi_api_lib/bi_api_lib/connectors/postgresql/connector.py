from bi_connector_postgresql.core.postgresql.connector import (
    PostgreSQLCoreConnector,
    PostgreSQLCoreConnectionDefinition,
    PostgreSQLTableCoreSourceDefinition,
    PostgreSQLSubselectCoreSourceDefinition,
)

from bi_api_connector.connector import (
    BiApiSourceDefinition,
    BiApiConnectionDefinition,
    BiApiConnector,
)
from bi_api_connector.api_schema.source_base import (
    SchematizedSQLDataSourceSchema, SchematizedSQLDataSourceTemplateSchema,
    SubselectDataSourceSchema, SubselectDataSourceTemplateSchema,
)

from bi_connector_postgresql.formula.constants import DIALECT_NAME_POSTGRESQL
from bi_api_lib.connectors.postgresql.api_schema.connection import PostgreSQLConnectionSchema
from bi_api_lib.connectors.postgresql.connection_form.form_config import PostgreSQLConnectionFormFactory
from bi_api_lib.connectors.postgresql.connection_info import PostgreSQLConnectionInfoProvider


class PostgreSQLBiApiTableSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = PostgreSQLTableCoreSourceDefinition
    api_schema_cls = SchematizedSQLDataSourceSchema
    template_api_schema_cls = SchematizedSQLDataSourceTemplateSchema


class PostgreSQLBiApiSubselectSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = PostgreSQLSubselectCoreSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class PostgreSQLBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = PostgreSQLCoreConnectionDefinition
    api_generic_schema_cls = PostgreSQLConnectionSchema
    info_provider_cls = PostgreSQLConnectionInfoProvider
    form_factory_cls = PostgreSQLConnectionFormFactory


class PostgreSQLBiApiConnector(BiApiConnector):
    core_connector_cls = PostgreSQLCoreConnector
    connection_definitions = (PostgreSQLBiApiConnectionDefinition,)
    source_definitions = (
        PostgreSQLBiApiTableSourceDefinition,
        PostgreSQLBiApiSubselectSourceDefinition,
    )
    formula_dialect_name = DIALECT_NAME_POSTGRESQL
    # translation_configs = frozenset(CONFIGS)  TODO: add after a connectorization
