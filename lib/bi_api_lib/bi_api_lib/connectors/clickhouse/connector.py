from bi_core.connectors.clickhouse.connector import (
    ClickHouseCoreConnector,
    ClickHouseCoreConnectionDefinition,
    ClickHouseTableCoreSourceDefinition,
    ClickHouseSubselectCoreSourceDefinition,
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
from bi_api_lib.connectors.clickhouse.api_schema.connection import ClickHouseConnectionSchema
from bi_api_lib.connectors.clickhouse.connection_form.form_config import ClickHouseConnectionFormFactory
from bi_api_lib.connectors.clickhouse.connection_info import ClickHouseConnectionInfoProvider


class ClickHouseBiApiTableSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = ClickHouseTableCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class ClickHouseBiApiSubselectSourceDefinition(BiApiSourceDefinition):
    core_source_def_cls = ClickHouseSubselectCoreSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class ClickHouseBiApiConnectionDefinition(BiApiConnectionDefinition):
    core_conn_def_cls = ClickHouseCoreConnectionDefinition
    api_generic_schema_cls = ClickHouseConnectionSchema
    info_provider_cls = ClickHouseConnectionInfoProvider
    form_factory_cls = ClickHouseConnectionFormFactory


class ClickHouseBiApiConnector(BiApiConnector):
    core_connector_cls = ClickHouseCoreConnector
    connection_definitions = (ClickHouseBiApiConnectionDefinition,)
    source_definitions = (
        ClickHouseBiApiTableSourceDefinition,
        ClickHouseBiApiSubselectSourceDefinition,
    )
    formula_dialect_name = DialectName.CLICKHOUSE
    # translation_configs = frozenset(CONFIGS)  TODO: add after a connectorization
