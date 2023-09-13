from bi_api_connector.connector import (
    BiApiSourceDefinition,
    BiApiConnectionDefinition,
    BiApiConnector,
)

from bi_api_connector.api_schema.source_base import (
    SQLDataSourceSchema, SQLDataSourceTemplateSchema, SubselectDataSourceSchema, SubselectDataSourceTemplateSchema,
)

from bi_connector_clickhouse.bi.api_schema.connection import ClickHouseConnectionSchema
from bi_connector_clickhouse.bi.connection_form.form_config import ClickHouseConnectionFormFactory
from bi_connector_clickhouse.bi.connection_info import ClickHouseConnectionInfoProvider
from bi_connector_clickhouse.bi.i18n.localizer import CONFIGS
from bi_connector_clickhouse.formula.constants import DIALECT_NAME_CLICKHOUSE
from bi_connector_clickhouse.core.clickhouse.connector import (
    ClickHouseCoreConnector,
    ClickHouseCoreConnectionDefinition,
    ClickHouseTableCoreSourceDefinition,
    ClickHouseSubselectCoreSourceDefinition,
)


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
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    translation_configs = frozenset(CONFIGS)
