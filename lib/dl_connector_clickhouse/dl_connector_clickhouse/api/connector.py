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
from dl_connector_clickhouse.core.clickhouse_base.connector import ClickHouseCoreBackendDefinition
from dl_connector_clickhouse.formula.constants import (
    DIALECT_NAME_CLICKHOUSE,
    ClickHouseDialect,
)


class ClickHouseApiTableSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = ClickHouseTableCoreSourceDefinition
    api_schema_cls = SQLDataSourceSchema
    template_api_schema_cls = SQLDataSourceTemplateSchema


class ClickHouseApiSubselectSourceDefinition(ApiSourceDefinition):
    core_source_def_cls = ClickHouseSubselectCoreSourceDefinition
    api_schema_cls = SubselectDataSourceSchema
    template_api_schema_cls = SubselectDataSourceTemplateSchema


class ClickHouseApiConnectionDefinition(ApiConnectionDefinition):
    core_conn_def_cls = ClickHouseCoreConnectionDefinition
    api_generic_schema_cls = ClickHouseConnectionSchema
    info_provider_cls = ClickHouseConnectionInfoProvider
    form_factory_cls = ClickHouseConnectionFormFactory


class ClickHouseApiBackendDefinition(ApiBackendDefinition):
    core_backend_definition = ClickHouseCoreBackendDefinition
    formula_dialect_name = DIALECT_NAME_CLICKHOUSE
    multi_query_mutation_factories = ApiBackendDefinition.multi_query_mutation_factories + (
        MQMFactorySettingItem(
            query_proc_mode=QueryProcessingMode.native_wf,
            dialects=ClickHouseDialect.and_above(ClickHouseDialect.CLICKHOUSE_22_10).to_list(),
            factory_cls=NoCompengMultiQueryMutatorFactory,
        ),
    )


class ClickHouseApiConnector(ApiConnector):
    backend_definition = ClickHouseApiBackendDefinition
    connection_definitions = (ClickHouseApiConnectionDefinition,)
    source_definitions = (
        ClickHouseApiTableSourceDefinition,
        ClickHouseApiSubselectSourceDefinition,
    )
    translation_configs = frozenset(CONFIGS)
