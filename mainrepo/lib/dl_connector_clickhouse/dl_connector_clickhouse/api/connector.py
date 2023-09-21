from dl_api_connector.api_schema.source_base import (
    SQLDataSourceSchema,
    SQLDataSourceTemplateSchema,
    SubselectDataSourceSchema,
    SubselectDataSourceTemplateSchema,
)
from dl_api_connector.connector import (
    BiApiConnectionDefinition,
    BiApiConnector,
    BiApiSourceDefinition,
)
from dl_connector_clickhouse.api.api_schema.connection import ClickHouseConnectionSchema
from dl_connector_clickhouse.api.connection_form.form_config import ClickHouseConnectionFormFactory
from dl_connector_clickhouse.api.connection_info import ClickHouseConnectionInfoProvider
from dl_connector_clickhouse.api.i18n.localizer import CONFIGS
from dl_connector_clickhouse.core.clickhouse.connector import (
    ClickHouseCoreConnectionDefinition,
    ClickHouseCoreConnector,
    ClickHouseSubselectCoreSourceDefinition,
    ClickHouseTableCoreSourceDefinition,
)
from dl_connector_clickhouse.formula.constants import DIALECT_NAME_CLICKHOUSE


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
