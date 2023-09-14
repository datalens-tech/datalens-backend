from bi_connector_oracle.core.connector import (
    OracleCoreConnector,
    OracleCoreConnectionDefinition,
    OracleTableCoreSourceDefinition,
    OracleSubselectCoreSourceDefinition,
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

from bi_connector_oracle.formula.constants import DIALECT_NAME_ORACLE
from bi_connector_oracle.bi.api_schema.connection import OracleConnectionSchema
from bi_connector_oracle.bi.connection_form.form_config import OracleConnectionFormFactory
from bi_connector_oracle.bi.connection_info import OracleConnectionInfoProvider
from bi_connector_oracle.bi.i18n.localizer import CONFIGS
from bi_connector_oracle.bi.dashsql import OracleDashSQLParamLiteralizer


class OracleBiApiTableSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = OracleTableCoreSourceDefinition
    api_schema_cls = SchematizedSQLDataSourceSchema
    template_api_schema_cls = SchematizedSQLDataSourceTemplateSchema


class OracleBiApiSubselectSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = OracleSubselectCoreSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class OracleBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = OracleCoreConnectionDefinition
    api_generic_schema_cls = OracleConnectionSchema
    info_provider_cls = OracleConnectionInfoProvider
    form_factory_cls = OracleConnectionFormFactory


class OracleBiApiConnector(BiApiConnector):
    core_connector_cls = OracleCoreConnector
    connection_definitions = (OracleBiApiConnectionDefinition,)
    source_definitions = (
        OracleBiApiTableSourceDefinition,
        OracleBiApiSubselectSourceDefinition,
    )
    formula_dialect_name = DIALECT_NAME_ORACLE
    translation_configs = frozenset(CONFIGS)
    dashsql_literalizer_cls = OracleDashSQLParamLiteralizer
